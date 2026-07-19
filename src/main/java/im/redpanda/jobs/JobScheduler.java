package im.redpanda.jobs;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
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

  public static ScheduledFuture<?> insert(Runnable runnable, long delayInMS) {
    // scheduleWithFixedDelay rejects a period <= 0. Jittered delays sampled
    // from [0, n] (e.g. OhResolveJob.DelayedSearchJob) can legitimately hit 0,
    // which must mean "as soon as possible", not an IllegalArgumentException.
    long delay = Math.max(1, delayInMS);
    return jobScheduler.scheduleWithFixedDelay(runnable, delay, delay, TimeUnit.MILLISECONDS);
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
