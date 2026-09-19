package im.redpanda.ops;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import java.security.Security;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * TD225: a throw out of a permanent job's {@link Job#work()} used to end that job. {@link
 * Job#done()} cancels the {@code ScheduledFuture}, so the 15-minute {@code SaveJobs} autosave
 * (LocalSettings, node cache, peers file) stopped for the rest of the process after a single
 * failing tick — one Sentry event, no retry, no log line saying the job is gone. Found while
 * reviewing #367: {@code NodeStore.saveToDisk()} lets the exception escape if both the disk-backed
 * and the memory-only rebuild fail.
 *
 * <p>A permanent job must therefore report the failure and keep its schedule; a non-permanent one
 * must keep the old behaviour (it has no later tick to recover in).
 */
class PermanentJobFailureTest {

  // not named serverContext: that would shadow the inherited Job.serverContext field inside
  // AlwaysFailingJob's constructor
  private static final ServerContext CONTEXT = ServerContext.buildDefaultServerContext();

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /** A job whose {@code work()} always throws, counting its invocations. */
  private static class AlwaysFailingJob extends Job {
    final AtomicInteger invocations = new AtomicInteger();
    final CountDownLatch invoked;

    AlwaysFailingJob(boolean permanent, long reRunDelay, int expectedInvocations) {
      super(CONTEXT, reRunDelay, permanent);
      this.invoked = new CountDownLatch(expectedInvocations);
    }

    @Override
    public void init() {}

    @Override
    public void work() {
      invocations.incrementAndGet();
      invoked.countDown();
      throw new IllegalStateException("TD225: work() failed on purpose");
    }
  }

  /**
   * Deterministic core of the fix, without the scheduler: two {@code run()} calls on a permanent
   * job whose {@code work()} throws must both reach {@code work()}, and the job must still be
   * registered afterwards. Registration is the observable proxy for the schedule: {@code done()}
   * deregisters the job and cancels the future in the same locked section, so a still-registered
   * job is one whose future was never cancelled.
   *
   * <p>Negative control: with the {@code if (!permanent)} guard removed the second {@code work()}
   * never runs (the {@code done} flag short-circuits {@code run()}), so both assertions fail.
   */
  @Test
  void permanentJobKeepsRunningAndStaysRegisteredAfterAThrow() {
    AlwaysFailingJob job = new AlwaysFailingJob(true, 60_000L, 2);
    CONTEXT.getJobRegistry().registerWithFreshId(job);
    Integer jobId = job.getJobId();

    job.run();
    job.run();

    assertThat(job.invocations).hasValue(2);
    assertThat(CONTEXT.getJobRegistry().get(jobId)).isSameAs(job);

    job.done();
  }

  /** A non-permanent job is still finished off by a throw — its failure has no next tick. */
  @Test
  void nonPermanentJobIsStillDoneAfterAThrow() {
    AlwaysFailingJob job = new AlwaysFailingJob(false, 60_000L, 1);
    CONTEXT.getJobRegistry().registerWithFreshId(job);
    Integer jobId = job.getJobId();

    job.run();
    assertThat(CONTEXT.getJobRegistry().get(jobId)).isNull();

    // the done flag short-circuits any further tick
    job.run();
    assertThat(job.invocations).hasValue(1);
  }

  /**
   * The same property through the real {@code ScheduledThreadPoolExecutor}, which is where the bug
   * actually bit: {@code scheduleWithFixedDelay}'s periodic task is cancelled both by {@code
   * future.cancel()} (the {@code done()} path) and by a {@code Throwable} escaping {@code run()},
   * so the fix only holds if {@code run()} swallows the failure itself.
   *
   * <p>Bounded wait on a latch rather than a sleep; 20 s against a 25 ms period is ~800 ticks of
   * headroom.
   */
  @Test
  void permanentJobKeepsItsScheduleAfterRepeatedThrows() throws Exception {
    AlwaysFailingJob job = new AlwaysFailingJob(true, 25L, 3);
    job.start();
    try {
      assertThat(job.invoked.await(20, TimeUnit.SECONDS)).isTrue();
    } finally {
      job.done();
    }
  }
}
