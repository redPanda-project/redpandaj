package im.redpanda.ops;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class JobScheduler extends ScheduledThreadPoolExecutor {

  private static JobScheduler jobScheduler;

  static {
    // ScheduledThreadPoolExecutor only uses a fixed thread pool with size
    // corePoolSize
    jobScheduler = new JobScheduler(8, new SimpleNamingThreadFactory());
  }

  public JobScheduler(int corePoolSize) {
    super(corePoolSize);
  }

  public JobScheduler(int corePoolSize, ThreadFactory threadFactory) {
    super(corePoolSize, threadFactory);
  }

  /**
   * Fraction of the period the first tick of a job is spread over: the initial delay is {@code
   * period + U[0, period / INITIAL_DELAY_JITTER_DIVISOR]}. 10 keeps the first tick within 110% of
   * the configured period, which is small enough that no caller's timing expectation changes.
   */
  static final long INITIAL_DELAY_JITTER_DIVISOR = 10;

  public static ScheduledFuture<?> insert(Runnable runnable, long delayInMS) {
    // scheduleWithFixedDelay rejects a period <= 0. Jittered delays sampled
    // from [0, n] (e.g. OhResolveJob.DelayedSearchJob) can legitimately hit 0,
    // which must mean "as soon as possible", not an IllegalArgumentException.
    long delay = Math.max(1, delayInMS);
    return jobScheduler.scheduleWithFixedDelay(
        runnable, initialDelayWithJitter(delay), delay, TimeUnit.MILLISECONDS);
  }

  /**
   * Initial delay for a job of the given period, jittered (TD224).
   *
   * <p>All permanent jobs used to be inserted with {@code initialDelay == period} within a few
   * milliseconds of each other, and their periods are multiples of one another (5 / 15 / 60 min).
   * Every hourly {@code ServerRestartJob} tick was therefore also a {@code SaveJobs} and {@code
   * NodeStoreMaintainJob} tick — permanently, for the whole life of the process. That is what
   * turned the {@code saveToDisk()}/{@code close()} race of TD221 (REDPANDAJ-2EZ) from a rare
   * coincidence into a certainty: all three testnet restarts of the night of 2026-09-05 landed on a
   * save tick (01:46:18, 02:46:18, 22:46:38).
   *
   * <p>Offsetting only the first tick is enough, because {@code scheduleWithFixedDelay} counts
   * every following period from the end of the previous run: the whole tick series of a job is
   * shifted by its jitter. Jitter (rather than a fixed offset per job) keeps the property
   * independent of the order in which {@code App} happens to start the jobs, and makes two jobs
   * whose periods are multiples of each other collide only for the width of the race window instead
   * of always.
   *
   * <p>Not {@link java.security.SecureRandom}: this is scheduling noise, nothing here is a secret.
   */
  static long initialDelayWithJitter(long period) {
    long span = period / INITIAL_DELAY_JITTER_DIVISOR;
    if (span <= 0) {
      // sub-divisor periods (e.g. the 1 ms clamp above) have no room to jitter in
      return period;
    }
    return period + ThreadLocalRandom.current().nextLong(span + 1);
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {}

  @Override
  protected void terminated() {}

  public static void runNow(Runnable command) {
    jobScheduler.execute(command);
  }

  static class SimpleNamingThreadFactory implements ThreadFactory {
    int number = 0;

    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setName("Jobs-" + number);
      number++;
      return thread;
    }
  }
}
