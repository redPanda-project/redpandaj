package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

/**
 * Test helper for "does this code path actually take lock X?" regressions.
 *
 * <p>Writing a racing stress test for a missing lock is inherently flaky: it only fails when the
 * interleaving happens to hit the window. This helper instead asserts the property directly and
 * deterministically — a code path that takes the lock cannot make progress while another thread
 * holds it in an incompatible mode.
 *
 * <p>The assertion is one-sided and therefore not timing-flaky: a slow machine only makes the
 * "still blocked" observation more likely, never less. The only way {@link #assertBlockedWhileHeld}
 * fails is if the action genuinely ran to completion without the lock, which is exactly the
 * regression being guarded against.
 */
public final class ConcurrencyTestSupport {

  /** How long the action must fail to complete while the lock is held. */
  private static final long MUST_STAY_BLOCKED_MS = 500;

  /** Upper bound for the action once the lock is released — generous, only guards a hang. */
  private static final long MUST_FINISH_SECONDS = 30;

  private ConcurrencyTestSupport() {}

  /**
   * Locks {@code blockingLock} on the calling thread, runs {@code action} on another thread and
   * asserts that it does not complete while the lock is held, then releases the lock and asserts
   * that the action completes (rethrowing anything it threw).
   *
   * @param blockingLock a lock held in a mode that conflicts with the lock the action must take
   *     (e.g. the write lock when the action is expected to take the read lock)
   * @param action the code path under test
   */
  public static void assertBlockedWhileHeld(Lock blockingLock, Runnable action) throws Exception {
    ExecutorService executor =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "lock-probe");
              thread.setDaemon(true);
              return thread;
            });
    boolean held = false;
    try {
      blockingLock.lock();
      held = true;

      CountDownLatch started = new CountDownLatch(1);
      Future<?> probe =
          executor.submit(
              () -> {
                started.countDown();
                action.run();
              });
      assertTrue(
          started.await(MUST_FINISH_SECONDS, TimeUnit.SECONDS), "probe thread never started");

      try {
        probe.get(MUST_STAY_BLOCKED_MS, TimeUnit.MILLISECONDS);
        fail("action completed while the conflicting lock was held — it does not take the lock");
      } catch (TimeoutException expected) {
        // still blocked on the lock, as required
      }

      blockingLock.unlock();
      held = false;

      // rethrows (wrapped) whatever the action threw
      probe.get(MUST_FINISH_SECONDS, TimeUnit.SECONDS);
    } finally {
      if (held) {
        blockingLock.unlock();
      }
      executor.shutdownNow();
    }
  }
}
