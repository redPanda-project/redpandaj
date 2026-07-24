package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression tests for TD013 (review-dag finding on redpandaj#271): {@link
 * InboundCommandProcessor#parseCommand} only catches {@code InvalidProtocolBufferException} from a
 * command handler — any other {@code RuntimeException} a handler throws propagates through {@code
 * loopCommands}'s {@code finally}-compact, through {@code
 * ConnectionReaderThread#readConnection(Peer)}'s claim/restore {@code finally} (REDPANDAJ-2EF), all
 * the way out to {@code run()}'s generic {@code catch (Throwable e)}. The documented contract "no
 * buffer leak even on a handler exception" was only ever tested on the disconnect-mid-parse half
 * (see {@code
 * ReadConnectionOwnershipHandoffTest#disconnectWhileBufferClaimedReturnsBufferExactlyOnce}), never
 * on the handler-throws half.
 *
 * <p>{@code commandHandlers} is a private field of a private nested functional interface, so there
 * is no source-level way to install a throwing stub from outside {@link InboundCommandProcessor}.
 * Rather than widen production visibility for a single test (no seam is actually unavoidable here),
 * a {@link Proxy} implementing the private interface — obtained purely via {@link
 * Class#getDeclaredClasses()} / {@link InvocationHandler}, which never needs to name the private
 * type — is swapped into the real handler map through reflection. This changes no production code
 * at all.
 */
public class ReadConnectionHandlerExceptionTest {

  static {
    ByteBufferPool.init();
  }

  private ServerSocketChannel serverSocket;
  private SocketChannel remoteSide;
  private SocketChannel peerSide;

  @Before
  public void setUpChannels() throws IOException {
    serverSocket = ServerSocketChannel.open();
    serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
    remoteSide = SocketChannel.open(serverSocket.getLocalAddress());
    peerSide = serverSocket.accept();
  }

  @After
  public void tearDownChannels() throws IOException {
    for (SocketChannel channel : new SocketChannel[] {remoteSide, peerSide}) {
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
    }
    if (serverSocket != null && serverSocket.isOpen()) {
      serverSocket.close();
    }
  }

  private Peer newConnectedPeer() {
    Peer peer = new Peer("127.0.0.1", 0, new NodeId());
    peer.setSocketChannel(peerSide);
    peer.setConnected(true);
    peer.writeBuffer = ByteBuffer.allocate(1024 * 100);
    peer.writeBufferCrypted = ByteBuffer.allocate(1024 * 100);
    // Same 32-byte key for both directions so the peer's receive stream decrypts frames encrypted
    // by its own send stream (loopback test trick, see PeerTest /
    // ReadConnectionOwnershipHandoffTest).
    byte[] key = new byte[32];
    peer.setPeerChiperStreams(new GcmFramedStreams(key, key));
    return peer;
  }

  private void sendEncrypted(Peer peer, byte[] plaintext) throws IOException {
    ByteBuffer in = ByteBuffer.allocate(plaintext.length);
    in.put(plaintext);
    in.flip();
    ByteBuffer out = ByteBuffer.allocate(plaintext.length + 1024);
    peer.getPeerChiperStreams().encrypt(in, out);
    out.flip();
    while (out.hasRemaining()) {
      remoteSide.write(out);
    }
  }

  /**
   * Replaces the {@code Command.PING} handler of a fresh {@link ConnectionReaderThread}'s internal
   * {@link InboundCommandProcessor} with a stub that unconditionally throws, then returns the
   * reader thread. Reflection only — no change to production visibility.
   */
  private ConnectionReaderThread newReaderThreadWithThrowingPingHandler(RuntimeException toThrow)
      throws Exception {
    ConnectionReaderThread readerThread =
        new ConnectionReaderThread(new ServerContext(), ConnectionReaderThread.STD_TIMEOUT);

    Field inboundProcessorField = ConnectionReaderThread.class.getDeclaredField("inboundProcessor");
    inboundProcessorField.setAccessible(true);
    InboundCommandProcessor inboundProcessor =
        (InboundCommandProcessor) inboundProcessorField.get(readerThread);

    Class<?> commandHandlerInterface = null;
    for (Class<?> declared : InboundCommandProcessor.class.getDeclaredClasses()) {
      if (declared.getSimpleName().equals("CommandHandler")) {
        commandHandlerInterface = declared;
        break;
      }
    }
    assertThat(commandHandlerInterface)
        .as("InboundCommandProcessor.CommandHandler nested interface must still exist")
        .isNotNull();

    InvocationHandler throwing =
        (proxy, method, args) -> {
          throw toThrow;
        };
    Object throwingHandler =
        Proxy.newProxyInstance(
            InboundCommandProcessor.class.getClassLoader(),
            new Class<?>[] {commandHandlerInterface},
            throwing);

    Field commandHandlersField = InboundCommandProcessor.class.getDeclaredField("commandHandlers");
    commandHandlersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<Byte, Object> commandHandlers =
        (Map<Byte, Object>) commandHandlersField.get(inboundProcessor);
    commandHandlers.put(Command.PING, throwingHandler);

    return readerThread;
  }

  /**
   * A single PING command exactly fills (and fully drains) the claimed buffer: the handler throws
   * before returning, so {@code loopCommands}'s {@code finally}-compact runs on a buffer with
   * nothing left unread. {@code readConnection}'s own claim/restore {@code finally} then sees
   * {@code position() == 0} ("drained") and must return the claimed buffer to the pool — not leak
   * it into {@code peer.readBuffer} — even though the exception is still propagating past it.
   */
  @Test
  public void handlerExceptionOnFullyDrainedBufferReturnsBufferToPoolWithoutLeak()
      throws Exception {
    Peer peer = newConnectedPeer();
    ByteBuffer preBorrowed = ByteBufferPool.borrowObject(1024);
    peer.readBuffer = preBorrowed;

    RuntimeException boom = new IllegalStateException("TD013 handler stub failure");
    ConnectionReaderThread readerThread = newReaderThreadWithThrowingPingHandler(boom);

    sendEncrypted(peer, new byte[] {Command.PING});

    assertThatThrownBy(() -> readerThread.readConnection(peer))
        .as("the handler exception must propagate all the way out of readConnection() unmasked")
        .isSameAs(boom);

    assertThat(peer.readBuffer)
        .as(
            "a fully drained buffer must be returned to the pool, not kept in (or leaked out of)"
                + " the field, even when the handler threw")
        .isNull();
    // LIFO pool: the instance we just returned is the first one handed out again, in pristine
    // state (position 0, limit == capacity) — proves it was returned exactly once, not corrupted.
    ByteBuffer reBorrowed = ByteBufferPool.borrowObject(1024);
    assertThat(reBorrowed).isSameAs(preBorrowed);
    assertThat(reBorrowed.position()).isZero();
    assertThat(reBorrowed.limit()).isEqualTo(reBorrowed.capacity());
    ByteBufferPool.returnObject(reBorrowed);
  }

  /**
   * Two PING command bytes arrive in one read. The throwing stub is installed before either is
   * parsed, so it fires on the very first one — but because a second, still-unread command byte
   * sits behind it in the buffer, the {@code finally}-compact in {@code loopCommands} leaves the
   * buffer with leftover bytes (position != 0 after compact) instead of fully drained. {@code
   * readConnection}'s claim/restore {@code finally} must then restore the SAME buffer instance into
   * {@code peer.readBuffer} (not drop it, not double-return it) so a later read can retry the
   * still-unparsed second command.
   */
  @Test
  public void handlerExceptionWithLeftoverBytesRestoresBufferIntoField() throws Exception {
    Peer peer = newConnectedPeer();
    ByteBuffer preBorrowed = ByteBufferPool.borrowObject(1024);
    peer.readBuffer = preBorrowed;

    RuntimeException boom = new IllegalStateException("TD013 handler stub failure");
    ConnectionReaderThread readerThread = newReaderThreadWithThrowingPingHandler(boom);

    sendEncrypted(peer, new byte[] {Command.PING, Command.PING});

    assertThatThrownBy(() -> readerThread.readConnection(peer)).isSameAs(boom);

    assertThat(peer.readBuffer)
        .as(
            "leftover (unparsed) bytes after a handler exception must be restored into the field,"
                + " same instance, not dropped or leaked")
        .isSameAs(preBorrowed);
    assertThat(peer.readBuffer.position())
        .as("the second, unparsed PING command byte must still be buffered (write mode)")
        .isEqualTo(1);
    assertThat(peer.isConnected())
        .as("a handler exception alone must not disconnect the peer")
        .isTrue();

    // cleanup via the production path
    peer.disconnect("test cleanup");
    assertThat(peer.readBuffer).isNull();
  }
}
