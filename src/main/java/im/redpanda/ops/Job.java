package im.redpanda.ops;

import im.redpanda.core.ServerContext;
import java.security.SecureRandom;
import java.util.concurrent.ScheduledFuture;

public abstract class Job implements Runnable {

  // public static final long RERUNTIME = 500L;
  public static final SecureRandom rand = new SecureRandom();

  protected ServerContext serverContext;

  private long reRunDelay = 500L; // default value
  private boolean permanent = false;
  private boolean skipImminentRun = false;

  int jobId = -1;
  private int runCounter = 0;
  private ScheduledFuture<?> future;
  private boolean done = false;
  protected boolean initilized = false;

  public Job(ServerContext serverContext) {
    this.serverContext = serverContext;
  }

  public Job(ServerContext serverContext, long reRunDelay) {
    this.serverContext = serverContext;
    this.reRunDelay = reRunDelay;
  }

  public Job(ServerContext serverContext, long reRunDelay, boolean permanent) {
    this.serverContext = serverContext;
    this.reRunDelay = reRunDelay;
    this.permanent = permanent;
  }

  public Job(
      ServerContext serverContext, long reRunDelay, boolean permanent, boolean skipImminentRun) {
    this.serverContext = serverContext;
    this.reRunDelay = reRunDelay;
    this.permanent = permanent;
    this.skipImminentRun = skipImminentRun;
  }

  @Override
  public void run() {

    if (!permanent && getEstimatedRuntime() > 60000L) {
      // if this job takes too long, lets finish
      System.out.println("job max time reached: " + jobId + " " + this.getClass().getName());
      done();
    }

    // lets run the init inside the run loop such that the init is runs in the
    // threadpool
    // and not in the creating thread
    if (!initilized) {
      initilized = true;
      try {
        init();
      } catch (Throwable e) {
        e.printStackTrace();
        Log.sentry(e);
        done();
        return;
      }
    }

    runCounter++;

    if (!initilized || done || (skipImminentRun && runCounter == 1)) {
      return;
    }

    try {
      work();
    } catch (Throwable e) {
      handleWorkFailure(e);
      return;
    }
    // count after doing the work, since the first start of the job is immediately

  }

  /**
   * Reports a failed {@link #work()} and decides whether the job is over.
   *
   * <p>TD225: a throw out of a <b>permanent</b> job's {@code work()} must not end that job. {@link
   * #done()} cancels the {@code ScheduledFuture}, so a single failing tick used to stop the job for
   * the rest of the process — one Sentry event and then silence. For {@code SaveJobs} that meant
   * the 15-minute autosave of {@code LocalSettings}, the node cache and the peers file never ran
   * again (found while reviewing #367, where {@code NodeStore.saveToDisk()} can throw if both the
   * disk-backed and the memory-only rebuild fail). A permanent job keeps its schedule and retries
   * on the next tick; a non-permanent job keeps the old behaviour, because for it a failed {@code
   * work()} has no later tick to recover in and it would otherwise be retried forever.
   *
   * <p>The reporting itself is wrapped, because {@code ScheduledThreadPoolExecutor} cancels a
   * periodic task whose {@code run()} throws: letting a failure of {@code Log.sentry} (e.g. its
   * {@code rating} counter not initialised because {@code Log.init} never ran) escape would
   * silently stop the very job this method is trying to keep alive.
   */
  private void handleWorkFailure(Throwable e) {
    try {
      e.printStackTrace(); // NOSONAR (java:S4507): controlled console output, mirrors init()
      Log.sentry(e);
    } catch (Throwable reportingFailure) {
      reportingFailure.printStackTrace(); // NOSONAR (java:S4507): last-resort diagnostics
    }
    if (!permanent) {
      done();
    }
  }

  /**
   * call this method if some data has been updated for this job and you do not want to wait till
   * the next rerun occurs by delay
   */
  public void updated() {
    // do not rise the runCounter, because this is an additional run beside the
    // returning rerun by
    // delay
    JobScheduler.runNow(this);
  }

  /**
   * stuff to be done before the work method is called, this code only runs once at the beginning
   * put stuff here so it will run in the threadpool and not in the calling thread
   */
  public abstract void init();

  /** work to do for this job, call done() if finished! */
  public abstract void work();

  public void start() {

    // the registry draws the id under its own lock, so it cannot collide with a running job
    serverContext.getJobRegistry().registerWithFreshId(this);

    // run delayed recurrent
    future = JobScheduler.insert(this, reRunDelay);

    // run immediately
    JobScheduler.runNow(this);
  }

  /**
   * estimated time in ms
   *
   * @return
   */
  public long getEstimatedRuntime() {
    return reRunDelay * runCounter;
  }

  /**
   * call this method if the job is finished, does the necessary cleanup, ie remove from running
   * jobs and cancel the future (obtained from scheduleWithFixedDelay)
   */
  public void done() {
    // The done-flag check and the removal from runningJobs must be atomic. Previously the
    // `if (done)` guard ran outside runningJobsLock while the actual removal (stopJob) ran under
    // it, so two threads (e.g. the init/timeout path and an inbound answer being processed for
    // the same job) could both observe done==false, both set it and both perform the removal;
    // the second one then found the job already gone and threw "CODE 17dh6" (REDPANDAJ-2E2 /
    // REDPANDAJ-2EA). Holding the lock across the whole check-and-remove makes done() idempotent:
    // a second, losing call simply returns. The former debug-only throw is dropped — a
    // double-done is benign cleanup, not a condition worth crashing the job thread over.
    JobRegistry registry = serverContext.getJobRegistry();
    registry.lock();
    try {
      if (done) {
        return;
      }
      done = true;

      Job removed = registry.removeLocked(jobId);
      if (removed != null && future != null) {
        future.cancel(false);
      }
    } catch (Throwable e) {
      Log.sentry(e);
    } finally {
      registry.unlock();
    }
  }

  public void setReRunDelay(long newDelay) {
    if (this.reRunDelay == newDelay) {
      return;
    }
    this.reRunDelay = newDelay;
    future.cancel(false);
    future = JobScheduler.insert(this, reRunDelay);
  }

  public long getReRunDelay() {
    return reRunDelay;
  }

  public Integer getJobId() {
    return jobId;
  }
}
