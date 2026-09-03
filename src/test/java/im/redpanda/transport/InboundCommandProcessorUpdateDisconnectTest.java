package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import im.redpanda.core.Command;
import im.redpanda.updater.UpdateTransfer;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for L4 (bug hunt 2026-07-26): the update-distribution runnables dereferenced
 * {@code peer.writeBuffer} after a {@code Thread.sleep} of up to 60 s and a multi-megabyte disk
 * read. {@link Peer#disconnect(String)} nulls that field, so a peer disconnecting in that window
 * raised an unchecked NPE inside a {@code Runnable} whose {@code Future} nobody observes — no log,
 * no Sentry. Two of the sites additionally did {@code lock(); put(); unlock();} without a {@code
 * finally}, so the NPE left {@code writeBufferLock} locked forever.
 *
 * <p>The helpers moved to {@link UpdateTransfer} with the updater package (T116); this test stays
 * in {@code im.redpanda.core} because it asserts on {@code Peer}'s package-private buffer fields.
 */
class InboundCommandProcessorUpdateDisconnectTest {

  @Test
  void requestUpdateContent_abortsCleanlyWhenThePeerDisconnected() {
    Peer peer = new Peer("127.0.0.1", 1234);
    peer.writeBuffer = null; // what disconnect() leaves behind

    assertThat(UpdateTransfer.requestUpdateContent(peer, Command.UPDATE_REQUEST_CONTENT)).isFalse();
    assertThat(peer.writeBufferLock.isLocked())
        .as("the write buffer lock must not be left held")
        .isFalse();
  }

  @Test
  void requestUpdateContent_writesTheCommandWhenConnected() {
    Peer peer = new Peer("127.0.0.1", 1234);
    peer.writeBuffer = ByteBuffer.allocate(64);

    assertThat(UpdateTransfer.requestUpdateContent(peer, Command.UPDATE_REQUEST_CONTENT)).isTrue();

    peer.writeBuffer.flip();
    assertThat(peer.writeBuffer.get()).isEqualTo(Command.UPDATE_REQUEST_CONTENT);
    assertThat(peer.writeBufferLock.isLocked()).isFalse();
  }

  @Test
  void appendToWriteBuffer_abortsCleanlyWhenThePeerDisconnected() {
    Peer peer = new Peer("127.0.0.1", 1234);
    peer.writeBuffer = null;

    ByteBuffer frame = ByteBuffer.allocate(8);
    frame.putLong(42L);
    frame.flip();

    assertThat(UpdateTransfer.appendToWriteBuffer(peer, frame)).isFalse();
    assertThat(peer.writeBufferLock.isLocked()).isFalse();
  }

  @Test
  void appendToWriteBuffer_growsTheBufferWhenTheFrameDoesNotFit() {
    Peer peer = new Peer("127.0.0.1", 1234);
    peer.writeBuffer = ByteBuffer.allocate(4);

    byte[] payload = new byte[64];
    payload[0] = 7;
    ByteBuffer frame = ByteBuffer.wrap(payload);

    assertThat(UpdateTransfer.appendToWriteBuffer(peer, frame)).isTrue();
    assertThat(peer.writeBuffer.capacity()).isGreaterThan(payload.length);
    assertThat(peer.writeBuffer.position()).isEqualTo(payload.length);
    assertThat(peer.writeBuffer.get(0)).isEqualTo((byte) 7);
  }

  /** An unchecked failure in a submitted update task must be reported, not swallowed. */
  @Test
  void reporting_absorbsAndReportsUncheckedExceptions() {
    Runnable wrapped =
        UpdateTransfer.reporting(
            "unit-test",
            () -> {
              throw new NullPointerException("peer disconnected mid-upload");
            });

    assertThatCode(wrapped::run).doesNotThrowAnyException();
  }

  /** Errors stay fatal — they are reported and rethrown. */
  @Test
  void reporting_rethrowsErrors() {
    Runnable wrapped =
        UpdateTransfer.reporting(
            "unit-test",
            () -> {
              throw new StackOverflowError("boom");
            });

    assertThatCode(wrapped::run).isInstanceOf(StackOverflowError.class);
  }
}
