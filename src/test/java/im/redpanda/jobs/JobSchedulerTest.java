package im.redpanda.jobs;

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.ScheduledFuture;
import org.junit.Test;

public class JobSchedulerTest {

  /**
   * Jittered job delays sampled from [0, n] can hit 0 (seen as a flaky IllegalArgumentException
   * from OhResolveJob.DelayedSearchJob in CI): insert() must clamp instead of letting
   * scheduleWithFixedDelay reject the period.
   */
  @Test
  public void insertAcceptsZeroDelay() {
    ScheduledFuture<?> future = JobScheduler.insert(() -> {}, 0);
    assertNotNull(future);
    future.cancel(false);
  }
}
