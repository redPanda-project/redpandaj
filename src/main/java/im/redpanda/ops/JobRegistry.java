package im.redpanda.ops;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The running {@link Job}s of one node, keyed by their random job id.
 *
 * <p>The map used to be {@code static} on {@code Job}, so two {@code ServerContext}s in one JVM
 * shared it and a peer's ACK reaching node A could resolve to a job of node B (the ids are random
 * ints, so a collision is unlikely — but the scope was wrong, and the multi-node E2E tests need the
 * boundary to hold). T118 moved it here; a registry is created per {@code ServerContext}.
 *
 * <p>The lock is exposed to {@link Job} (package-private) rather than wrapped, because {@code
 * Job.done()} must hold it across its whole check-and-remove: the {@code done} flag check and the
 * removal have to be atomic or two threads both deregister the job and the loser throws (REDPANDAJ-
 * 2E2 / REDPANDAJ-2EA, pinned by {@code JobDoneIdempotencyTest}).
 */
public final class JobRegistry {

  private final Map<Integer, Job> runningJobs = new HashMap<>(10);
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Registers {@code job} under a fresh id that is not in use in this registry, and stores that id
   * on the job.
   *
   * <p>Drawing the id under the lock is what makes it unique: {@code start()} used to pick {@code
   * rand.nextInt()} and {@code put} it blindly, so a collision (unlikely, but not impossible over a
   * node's lifetime) silently replaced the older job in the map — its ACK would then have been
   * routed to the newer one and its {@code done()} would have deregistered a job it does not own.
   */
  void registerWithFreshId(Job job) {
    lock.lock();
    try {
      int jobId;
      do {
        jobId = Job.rand.nextInt();
      } while (runningJobs.containsKey(jobId));
      job.jobId = jobId;
      runningJobs.put(jobId, job);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Registers a job under a given id, replacing whatever was registered under it.
   *
   * <p>Public only for tests that need a deterministic job id; production code uses {@link
   * #registerWithFreshId(Job)}.
   */
  public void register(int jobId, Job job) {
    lock.lock();
    try {
      runningJobs.put(jobId, job);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Retrieves a running job by id, used to correlate a peer's answer with the job that asked.
   *
   * @return the job, or {@code null} if it is not (or no longer) running on this node
   */
  public Job get(int jobId) {
    lock.lock();
    try {
      return runningJobs.get(jobId);
    } finally {
      lock.unlock();
    }
  }

  void lock() {
    lock.lock();
  }

  void unlock() {
    lock.unlock();
  }

  /** Callers must hold {@link #lock()}. */
  Job removeLocked(int jobId) {
    return runningJobs.remove(jobId);
  }
}
