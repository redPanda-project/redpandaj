package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression tests for L3 (bug hunt 2026-07-26): {@code byteBufferToStacktrace} recorded a full
 * stack-trace string on every successful {@code returnObject()} and was never pruned.
 * commons-pool's evictor destroys idle buffers after 30 s, but the map entries — and their
 * multi-kilobyte strings — stayed strongly reachable for the whole process lifetime.
 */
public class ByteBufferPoolStacktraceTrackingTest {

  private boolean previousTraceReturns;

  @Before
  public void setUp() {
    ByteBufferPool.init();
    previousTraceReturns = ByteBufferPool.setTraceReturnsForTest(false);
  }

  @After
  public void tearDown() {
    ByteBufferPool.setTraceReturnsForTest(previousTraceReturns);
  }

  /** The debug facility is opt-in: nothing is captured or retained on the hot return path. */
  @Test
  public void returnObject_recordsNothingWhenTracingIsDisabled() {
    ByteBufferPool.setTraceReturnsForTest(false);

    ByteBuffer buffer = ByteBufferPool.borrowObject(512);
    ByteBufferPool.returnObject(buffer);

    assertThat(ByteBufferPool.isTraced(buffer)).isFalse();
  }

  /** With tracing on the entry must not outlive the buffer it describes. */
  @Test
  public void tracedStacktraceIsDroppedWhenThePoolDestroysTheBuffer() {
    ByteBufferPool.setTraceReturnsForTest(true);

    ByteBuffer buffer = ByteBufferPool.borrowObject(512);
    ByteBufferPool.returnObject(buffer);

    assertThat(ByteBufferPool.isTraced(buffer))
        .as("tracing is on, so the return stack must be recorded")
        .isTrue();

    // clear() destroys every idle object exactly like the evictor does after 30 s of idleness
    ByteBufferPool.getPool().clear();

    assertThat(ByteBufferPool.isTraced(buffer))
        .as("the recorded stack must be dropped together with the destroyed buffer")
        .isFalse();
  }
}
