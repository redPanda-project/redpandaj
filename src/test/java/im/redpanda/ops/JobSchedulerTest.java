package im.redpanda.ops;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.ScheduledFuture;
import org.junit.jupiter.api.Test;

class JobSchedulerTest {

  /**
   * Jittered job delays sampled from [0, n] can hit 0 (seen as a flaky IllegalArgumentException
   * from OhResolveJob.DelayedSearchJob in CI): insert() must clamp instead of letting
   * scheduleWithFixedDelay reject the period.
   */
  @Test
  void insertAcceptsZeroDelay() {
    ScheduledFuture<?> future = JobScheduler.insert(() -> {}, 0);
    assertNotNull(future);
    future.cancel(false);
  }
}
