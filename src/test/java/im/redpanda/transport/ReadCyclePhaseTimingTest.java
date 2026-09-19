package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.Command;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * TD226: the read-cycle watchdog of {@link ConnectionReaderThread#run()} reports its phases
 * separately now, and the numbers are only worth anything if each phase is actually measured where
 * it happens and the values never survive into the next cycle. {@code SlowReadCycleMessageTest}
 * pins the format, this pins the wiring — over a real loopback read, the same harness {@link
 * ReadConnectionOwnershipHandoffTest} uses.
 */
class ReadCyclePhaseTimingTest {

  static {
    ByteBufferPool.init();
  }

  private ServerSocketChannel serverSocket;
  private SocketChannel remoteSide;
  private SocketChannel peerSide;

  @BeforeEach
  void setUpChannels() throws IOException {
    serverSocket = ServerSocketChannel.open();
    serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
    remoteSide = SocketChannel.open(serverSocket.getLocalAddress());
    peerSide = serverSocket.accept();
  }

  @AfterEach
  void tearDownChannels() throws IOException {
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
   * A complete read cycle must populate every phase, and an early return afterwards must reset them
   * instead of reporting the previous cycle's numbers — otherwise the next REDPANDAJ-2DQ event
   * would blame a phase that did not run at all.
   */
  @Test
  void everyPhaseIsMeasuredAndResetOnTheNextCycle() throws Exception {
    Peer peer = newConnectedPeer();
    ConnectionReaderThread reader =
        new ConnectionReaderThread(new ServerContext(), ConnectionReaderThread.STD_TIMEOUT);

    sendEncrypted(peer, new byte[] {Command.PONG, Command.PONG, Command.PONG});
    assertThat(reader.readConnection(peer)).isGreaterThan(0);

    assertThat(reader.lastReadNanos).as("socket read").isPositive();
    assertThat(reader.lastLockWaitNanos).as("readBuffer lock wait").isNotNegative();
    assertThat(reader.lastDecryptNanos).as("decrypt").isPositive();
    assertThat(reader.lastDispatchNanos).as("dispatch").isPositive();

    // Second cycle, dropped inside the lock because the connection is gone (the stale-connection
    // guard): the read and the lock wait happen, decrypt and dispatch do not.
    sendEncrypted(peer, new byte[] {Command.PONG});
    peer.setConnected(false);
    assertThat(reader.readConnection(peer)).isGreaterThan(0);

    assertThat(reader.lastReadNanos).as("socket read of the second cycle").isPositive();
    assertThat(reader.lastDecryptNanos)
        .as("decrypt did not run in the second cycle, so it must not be reported")
        .isZero();
    assertThat(reader.lastDispatchNanos)
        .as("dispatch did not run in the second cycle, so it must not be reported")
        .isZero();
  }

  /**
   * The phase the whole row is about: time the reader spends blocked on the peer's {@code
   * writeBufferLock} has to land in the lock-wait number and nowhere else. A test that only checks
   * the format string would not notice a swapped argument at the call site (adversarial review of
   * this PR).
   */
  @Test
  void aContendedReadBufferLockIsReportedAsLockWait() throws Exception {
    Peer peer = newConnectedPeer();
    ConnectionReaderThread reader =
        new ConnectionReaderThread(new ServerContext(), ConnectionReaderThread.STD_TIMEOUT);
    sendEncrypted(peer, new byte[] {Command.PONG});

    peer.getWriteBufferLock().lock();
    Thread readerThread =
        new Thread(
            () -> {
              try {
                reader.readConnection(peer);
              } catch (PeerProtocolException e) {
                throw new IllegalStateException(e);
              }
            },
            "read-cycle-under-test");
    readerThread.start();

    // wait (without a timed sleep) until the reader is really queued on the lock, only then start
    // holding it for a measurable time
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (!peer.getWriteBufferLock().hasQueuedThreads() && System.nanoTime() < deadlineNanos) {
      Thread.onSpinWait();
    }
    assertThat(peer.getWriteBufferLock().hasQueuedThreads()).isTrue();

    Thread.sleep(250);
    peer.getWriteBufferLock().unlock();
    readerThread.join(TimeUnit.SECONDS.toMillis(20));

    assertThat(TimeUnit.NANOSECONDS.toMillis(reader.lastLockWaitNanos))
        .as("the 250 ms the lock was held must show up as lock wait")
        .isGreaterThanOrEqualTo(200L);
    assertThat(reader.lastDecryptNanos)
        .as("the decrypt of a single PONG cannot outlast the contended lock")
        .isLessThan(reader.lastLockWaitNanos);
    assertThat(reader.lastDispatchNanos)
        .as("dispatching a single PONG cannot outlast the contended lock")
        .isLessThan(reader.lastLockWaitNanos);
  }
}
