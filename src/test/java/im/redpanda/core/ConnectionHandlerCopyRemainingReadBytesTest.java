package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import org.junit.Test;

/**
 * Regression tests for TD014 (review-dag-Retest finding on redpandaj#271): {@link
 * ConnectionHandler#copyRemainingReadBytesToPeerBuffer(ByteBuffer, Peer)} is the locked borrow/put
 * path that hands handshake bytes which arrived coalesced AFTER the first PING frame
 * (REDPANDAJ-2DS) over to the peer's regular read buffer. No test in the repo reaches it with
 * {@code remaining() > 0}: {@code ConnectionHandlerCoalescedHandshakeTest} sends exactly the
 * coalesced PING frame and nothing more, so {@code tempHandshakeReadBuffer.hasRemaining()} is
 * always {@code false} there and the method takes its early return on the very first line. A
 * regression in the locked path itself (lock dropped, wrong branch taken, wrong append offset)
 * would go unnoticed.
 *
 * <p>The method is private; there is no source-level way to call it other than reflection (same
 * pattern already used by {@code ConnectionHandlerCoalescedHandshakeTest} for {@code
 * handlePeerInHandshake}), so both branches — {@code peer.readBuffer == null} (allocate + put) and
 * {@code peer.readBuffer != null} (append) — are driven directly with a hand-built {@code
 * tempHandshakeReadBuffer} in the same flipped ("read mode") state the real caller ({@code
 * handleFirstEncryptedCommand}) leaves it in after consuming the command byte.
 */
public class ConnectionHandlerCopyRemainingReadBytesTest {

  static {
    ByteBufferPool.init();
  }

  private Method copyRemainingReadBytesToPeerBufferMethod() throws NoSuchMethodException {
    Method method =
        ConnectionHandler.class.getDeclaredMethod(
            "copyRemainingReadBytesToPeerBuffer", ByteBuffer.class, Peer.class);
    method.setAccessible(true);
    return method;
  }

  private byte[] contentsOf(ByteBuffer writeMode) {
    ByteBuffer readable = writeMode.duplicate();
    readable.flip();
    byte[] out = new byte[readable.remaining()];
    readable.get(out);
    return out;
  }

  /**
   * {@code peer.readBuffer == null}: the method must borrow a fresh buffer from the pool sized to
   * the remaining bytes and put all of them into it.
   */
  @Test
  public void nullReadBufferAllocatesAndFillsFromPool() throws Exception {
    ConnectionHandler connectionHandler =
        new ConnectionHandler(ServerContext.buildDefaultServerContext(), false);
    Peer peer = new Peer("127.0.0.1", 0, new NodeId());
    assertThat(peer.readBuffer).as("precondition: no buffer claimed yet").isNull();

    byte[] leftoverPlaintext = {10, 20, 30};
    // Same "flipped, command byte already consumed" state handleFirstEncryptedCommand leaves the
    // buffer in before calling this method.
    ByteBuffer tempHandshakeReadBuffer = ByteBuffer.wrap(leftoverPlaintext);

    copyRemainingReadBytesToPeerBufferMethod()
        .invoke(connectionHandler, tempHandshakeReadBuffer, peer);

    assertThat(peer.readBuffer).as("a buffer must have been borrowed and stored").isNotNull();
    assertThat(peer.readBuffer.position())
        .as("all remaining bytes must have been put into the new buffer")
        .isEqualTo(leftoverPlaintext.length);
    assertThat(contentsOf(peer.readBuffer)).containsExactly(leftoverPlaintext);
    assertThat(tempHandshakeReadBuffer.hasRemaining())
        .as("the source buffer must be fully drained by the relative put")
        .isFalse();

    peer.disconnect("test cleanup"); // returns peer.readBuffer to the pool via the production path
    assertThat(peer.readBuffer).isNull();
  }

  /**
   * {@code peer.readBuffer != null}: the method must APPEND to the existing buffer (same instance,
   * prior bytes preserved, new bytes placed right after them) rather than overwrite it or allocate
   * a second one.
   */
  @Test
  public void nonNullReadBufferAppendsPreservingExistingContent() throws Exception {
    ConnectionHandler connectionHandler =
        new ConnectionHandler(ServerContext.buildDefaultServerContext(), false);
    Peer peer = new Peer("127.0.0.1", 0, new NodeId());

    ByteBuffer existing = ByteBufferPool.borrowObject(16);
    byte[] alreadyBuffered = {1, 2, 3};
    existing.put(alreadyBuffered);
    peer.readBuffer = existing;

    byte[] leftoverPlaintext = {4, 5};
    ByteBuffer tempHandshakeReadBuffer = ByteBuffer.wrap(leftoverPlaintext);

    copyRemainingReadBytesToPeerBufferMethod()
        .invoke(connectionHandler, tempHandshakeReadBuffer, peer);

    assertThat(peer.readBuffer)
        .as("must append into the SAME buffer instance, not allocate a new one")
        .isSameAs(existing);
    assertThat(peer.readBuffer.position())
        .as("position must advance past both the pre-existing and the newly appended bytes")
        .isEqualTo(alreadyBuffered.length + leftoverPlaintext.length);
    assertThat(contentsOf(peer.readBuffer)).containsExactly(1, 2, 3, 4, 5);

    peer.disconnect("test cleanup");
    assertThat(peer.readBuffer).isNull();
  }
}
