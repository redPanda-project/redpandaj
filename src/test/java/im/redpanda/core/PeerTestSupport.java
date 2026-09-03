package im.redpanda.core;

import java.nio.ByteBuffer;

/**
 * Test-only bridge to {@link Peer}'s package-private write buffer (T115).
 *
 * <p>{@code Peer.writeBuffer} and its lock are internal to {@code im.redpanda.core} so that the
 * routing, mailbox, DHT and updater code has to queue bytes through the {@code enqueue*} API, which
 * owns the locking. Tests in those packages still need to hand a peer a buffer and read back what
 * was written, and they are the only legitimate reason to reach past that boundary — so the access
 * lives here, in one clearly test-scoped place, instead of widening {@link Peer}'s API.
 */
public final class PeerTestSupport {

  private PeerTestSupport() {}

  /**
   * Gives the peer a write buffer, the way a real connection setup does — just with a size that
   * suits the test rather than the fixed 300 KiB of {@code setupConnectionForPeer}.
   *
   * @return the installed buffer
   */
  public static ByteBuffer initWriteBuffer(Peer peer, int capacity) {
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    peer.writeBuffer = buffer;
    return buffer;
  }

  /** The peer's live write buffer, for asserting on the bytes a production call queued. */
  public static ByteBuffer writeBuffer(Peer peer) {
    return peer.getWriteBuffer();
  }

  /** Gives the peer the ciphertext buffer a real connection setup allocates alongside. */
  public static ByteBuffer initWriteBufferCrypted(Peer peer, int capacity) {
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    peer.writeBufferCrypted = buffer;
    return buffer;
  }

  /** The peer's live ciphertext buffer. */
  public static ByteBuffer writeBufferCrypted(Peer peer) {
    return peer.writeBufferCrypted;
  }

  /** Marks the peer as authenticated, which outside of tests only the handshake does. */
  public static void setAuthed(Peer peer, boolean authed) {
    peer.setAuthed(authed);
  }
}
