package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import im.redpanda.core.Command;
import im.redpanda.ops.Settings;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for the frame API {@link Peer} gained in T115 — the one place that now owns {@code
 * writeBufferLock}, the {@code writeBuffer} null re-read and the buffer replacement that {@code
 * InboundCommandProcessor.appendToWriteBuffer} used to do from outside the class.
 */
class PeerFrameApiTest {

  private static Peer peerWithBuffer(int capacity) {
    Peer peer = new Peer("127.0.0.1", 59558);
    PeerTestSupport.initWriteBuffer(peer, capacity);
    return peer;
  }

  @Test
  void enqueueFrame_writesCommandLengthAndPayload() {
    Peer peer = peerWithBuffer(64);
    byte[] payload = {1, 2, 3};

    assertThat(peer.enqueueFrame(Command.FLASCHENPOST_PUT, payload)).isTrue();

    ByteBuffer buffer = PeerTestSupport.writeBuffer(peer);
    buffer.flip();
    assertThat(buffer.get()).isEqualTo(Command.FLASCHENPOST_PUT);
    assertThat(buffer.getInt()).isEqualTo(3);
    byte[] read = new byte[3];
    buffer.get(read);
    assertThat(read).isEqualTo(payload);
    assertThat(buffer.hasRemaining()).isFalse();
    assertThat(peer.getWriteBufferLock().isLocked()).isFalse();
  }

  @Test
  void enqueueCommand_writesOneByte() {
    Peer peer = peerWithBuffer(8);

    assertThat(peer.enqueueCommand(Command.PONG)).isTrue();

    assertThat(PeerTestSupport.writeBuffer(peer).position()).isEqualTo(1);
    assertThat(PeerTestSupport.writeBuffer(peer).get(0)).isEqualTo(Command.PONG);
  }

  /** What {@link Peer#disconnect(String)} leaves behind: no buffer, so nothing can be queued. */
  @Test
  void enqueueFrame_abortsCleanlyWhenTheBufferIsGone() {
    Peer peer = new Peer("127.0.0.1", 59558);

    assertThat(peer.enqueueFrame(Command.FLASCHENPOST_PUT, new byte[] {1})).isFalse();
    assertThat(peer.enqueueCommand(Command.PONG)).isFalse();
    assertThat(peer.enqueueGrowingFrame(ByteBuffer.allocate(4))).isFalse();
    assertThat(peer.writeBufferLocked(buffer -> buffer.put((byte) 1))).isFalse();
    assertThat(peer.getWriteBufferLock().isLocked())
        .as("the write buffer lock must not be left held")
        .isFalse();
  }

  /** A throwing writer must still release the lock — the missing {@code finally} of bug hunt L4. */
  @Test
  void writeBufferLocked_releasesTheLockWhenTheWriterThrows() {
    Peer peer = peerWithBuffer(4);

    assertThatThrownBy(
            () ->
                peer.writeBufferLocked(
                    buffer -> {
                      throw new IllegalStateException("boom");
                    }))
        .isInstanceOf(IllegalStateException.class);

    assertThat(peer.getWriteBufferLock().isLocked()).isFalse();
  }

  @Test
  void enqueueGrowingFrame_growsTheBufferAndKeepsThePendingBytes() {
    Peer peer = peerWithBuffer(8);
    assertThat(peer.enqueueCommand((byte) 42)).isTrue();
    ByteBuffer beforeGrowth = PeerTestSupport.writeBuffer(peer);

    byte[] large = new byte[64];
    large[0] = 7;
    large[63] = 9;

    assertThat(peer.enqueueGrowingFrame(ByteBuffer.wrap(large))).isTrue();

    ByteBuffer grown = PeerTestSupport.writeBuffer(peer);
    assertThat(grown).as("a frame that does not fit replaces the buffer").isNotSameAs(beforeGrowth);
    assertThat(grown.capacity()).isGreaterThanOrEqualTo(8 + 64);
    assertThat(grown.position()).isEqualTo(1 + 64);
    assertThat(grown.get(0)).as("the byte queued before the growth survives").isEqualTo((byte) 42);
    assertThat(grown.get(1)).isEqualTo((byte) 7);
    assertThat(grown.get(64)).isEqualTo((byte) 9);
  }

  @Test
  void enqueueGrowingFrame_keepsTheBufferWhenTheFrameFits() {
    Peer peer = peerWithBuffer(64);
    ByteBuffer buffer = PeerTestSupport.writeBuffer(peer);

    assertThat(peer.enqueueGrowingFrame(ByteBuffer.wrap(new byte[] {1, 2}))).isTrue();

    assertThat(PeerTestSupport.writeBuffer(peer)).isSameAs(buffer);
    assertThat(buffer.position()).isEqualTo(2);
  }

  @Test
  void tryEnqueueFrame_givesUpWhileTheLockIsHeldElsewhere() throws Exception {
    Peer peer = peerWithBuffer(64);
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder =
        new Thread(
            () -> {
              peer.getWriteBufferLock().lock();
              try {
                locked.countDown();
                release.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                peer.getWriteBufferLock().unlock();
              }
            },
            "lock-holder");
    holder.setDaemon(true);
    holder.start();
    assertThat(locked.await(30, TimeUnit.SECONDS)).isTrue();

    try {
      // one-sided assertion: a slow machine only makes the lock harder to get, never easier
      assertThat(
              peer.tryEnqueueFrame(Command.KADEMLIA_GET, new byte[] {1}, 20, TimeUnit.MILLISECONDS))
          .isFalse();
      assertThat(PeerTestSupport.writeBuffer(peer).position())
          .as("a frame that was not queued must not have written anything")
          .isZero();
    } finally {
      release.countDown();
      holder.join(TimeUnit.SECONDS.toMillis(30));
    }

    assertThat(peer.tryEnqueueFrame(Command.KADEMLIA_GET, new byte[] {1}, 30, TimeUnit.SECONDS))
        .as("once the lock is free the frame goes in")
        .isTrue();
  }

  @Test
  void hasQueuedOutboundBytes_reportsOnlyWhatIsActuallyStillQueued() {
    Peer peer = peerWithBuffer(64);
    peer.setConnected(true);
    PeerTestSupport.initWriteBufferCrypted(peer, 64);

    assertThat(peer.hasQueuedOutboundBytes()).as("nothing written yet").isFalse();

    assertThat(peer.enqueueCommand(Command.PONG)).isTrue();
    assertThat(peer.hasQueuedOutboundBytes()).as("plaintext waiting to be encrypted").isTrue();

    PeerTestSupport.writeBuffer(peer).clear();
    PeerTestSupport.writeBufferCrypted(peer).put((byte) 1);
    assertThat(peer.hasQueuedOutboundBytes()).as("ciphertext waiting for the socket").isTrue();

    peer.setConnected(false);
    assertThat(peer.hasQueuedOutboundBytes())
        .as("a peer that is gone has nothing queued, whatever is left in the buffers")
        .isFalse();
  }

  /** The buffers of a peer that disconnected are freed only once it has been silent long enough. */
  @Test
  void releaseWriteBuffersIfIdle_freesTheBuffersOfALongSilentDisconnectedPeer() {
    Peer peer = peerWithBuffer(64);
    PeerTestSupport.initWriteBufferCrypted(peer, 64);
    peer.setLastPongReceived(System.currentTimeMillis() - Settings.pingTimeout * 2 - 1000);

    peer.releaseWriteBuffersIfIdle();

    assertThat(PeerTestSupport.writeBuffer(peer)).isNull();
    assertThat(PeerTestSupport.writeBufferCrypted(peer)).isNull();
  }

  /**
   * The reap decision is made outside the lock and re-tested inside it: a peer that reconnected in
   * between must keep the fresh buffers its handshake just allocated (TD029/REDPANDAJ-2EJ).
   */
  @Test
  void releaseWriteBuffersIfIdle_keepsTheBuffersOfAConnectedPeer() {
    Peer peer = peerWithBuffer(64);
    PeerTestSupport.initWriteBufferCrypted(peer, 64);
    peer.setLastPongReceived(System.currentTimeMillis() - Settings.pingTimeout * 2 - 1000);
    peer.setConnected(true);

    peer.releaseWriteBuffersIfIdle();

    assertThat(PeerTestSupport.writeBuffer(peer)).isNotNull();
    assertThat(PeerTestSupport.writeBufferCrypted(peer)).isNotNull();
  }

  /** A peer that answered recently keeps its buffers even while disconnected. */
  @Test
  void releaseWriteBuffersIfIdle_keepsTheBuffersOfARecentlyAnsweringPeer() {
    Peer peer = peerWithBuffer(64);
    PeerTestSupport.initWriteBufferCrypted(peer, 64);
    peer.setLastPongReceived(System.currentTimeMillis());

    peer.releaseWriteBuffersIfIdle();

    assertThat(PeerTestSupport.writeBuffer(peer)).isNotNull();
    assertThat(PeerTestSupport.writeBufferCrypted(peer)).isNotNull();
  }

  /**
   * Concurrent producers are the reason the locking exists: the write buffer is a single shared
   * position, so two unsynchronised {@code put()} sequences would interleave and produce frames
   * whose length prefix does not match their payload — a desync of the whole connection, not a lost
   * message (a peer discards one byte per unknown command).
   */
  @Test
  void concurrentEnqueueFrame_keepsEveryFrameIntact() throws Exception {
    int threads = 8;
    int framesPerThread = 200;
    int payloadLength = 16;
    Peer peer = peerWithBuffer(threads * framesPerThread * (1 + 4 + payloadLength));

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger queued = new AtomicInteger();
    try {
      for (int t = 0; t < threads; t++) {
        byte marker = (byte) t;
        executor.submit(
            () -> {
              byte[] payload = new byte[payloadLength];
              java.util.Arrays.fill(payload, marker);
              start.await();
              for (int i = 0; i < framesPerThread; i++) {
                if (peer.enqueueFrame(Command.FLASCHENPOST_PUT, payload)) {
                  queued.incrementAndGet();
                }
              }
              return null;
            });
      }
      start.countDown();
      executor.shutdown();
      assertThat(executor.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
    } finally {
      executor.shutdownNow();
    }

    assertThat(queued.get()).isEqualTo(threads * framesPerThread);

    ByteBuffer buffer = PeerTestSupport.writeBuffer(peer);
    buffer.flip();
    Map<Byte, Integer> perMarker = new HashMap<>();
    while (buffer.hasRemaining()) {
      assertThat(buffer.get()).isEqualTo(Command.FLASCHENPOST_PUT);
      int length = buffer.getInt();
      assertThat(length)
          .as("a torn frame would carry a foreign length prefix")
          .isEqualTo(payloadLength);
      byte[] payload = new byte[length];
      buffer.get(payload);
      byte marker = payload[0];
      for (byte b : payload) {
        assertThat(b).as("two writers interleaved inside one frame").isEqualTo(marker);
      }
      perMarker.merge(marker, 1, Integer::sum);
    }

    assertThat(perMarker).hasSize(threads);
    assertThat(perMarker.values()).allMatch(count -> count == framesPerThread);
  }
}
