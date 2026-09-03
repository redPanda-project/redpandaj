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
   * Registers a job under a given id.
   *
   * <p>Public because {@code Job.start()} draws a random id, so tests that need a deterministic one
   * register the job themselves.
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
