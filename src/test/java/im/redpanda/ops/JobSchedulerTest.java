package im.redpanda.ops;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class JobSchedulerTest {

  private static final long SAVE_JOBS_PERIOD_MS = 15L * 60L * 1000L;
  private static final long SERVER_RESTART_PERIOD_MS = 60L * 60L * 1000L;

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

  /** TD224: the first tick may be spread, but only inside one tenth of the period. */
  @Test
  void initialDelayStaysWithinTheJitterWindow() {
    for (int i = 0; i < 1000; i++) {
      assertThat(JobScheduler.initialDelayWithJitter(SAVE_JOBS_PERIOD_MS))
          .isBetween(
              SAVE_JOBS_PERIOD_MS,
              SAVE_JOBS_PERIOD_MS
                  + SAVE_JOBS_PERIOD_MS / JobScheduler.INITIAL_DELAY_JITTER_DIVISOR);
    }
  }

  /**
   * The jitter has to be wired into {@code insert()}, not merely exist as a helper: reverting
   * {@code insert()} to {@code scheduleWithFixedDelay(delay, delay, ...)} would keep every other
   * test in this class green (adversarial review of this PR). {@code getDelay()} returns the
   * remaining delay, so anything above the bare period can only come from the jitter.
   */
  @Test
  void insertAppliesTheJitterToTheFirstTick() {
    List<ScheduledFuture<?>> futures = new ArrayList<>();
    try {
      boolean sawJitteredFirstTick = false;
      for (int i = 0; i < 50 && !sawJitteredFirstTick; i++) {
        ScheduledFuture<?> future = JobScheduler.insert(() -> {}, SERVER_RESTART_PERIOD_MS, true);
        futures.add(future);
        sawJitteredFirstTick = future.getDelay(TimeUnit.MILLISECONDS) > SERVER_RESTART_PERIOD_MS;
      }
      assertThat(sawJitteredFirstTick).isTrue();
    } finally {
      futures.forEach(future -> future.cancel(false));
    }
  }

  /**
   * The jitter must stay opt-in: one-shot jobs ({@code skipImminentRun}) sample their delay from a
   * documented {@code [0, max]} and that single run is a deadline, not a re-run interval, so the
   * default {@code insert()} has to schedule the first tick exactly one period away (Copilot review
   * of this PR).
   */
  @Test
  void insertDoesNotJitterUnlessAskedTo() {
    List<ScheduledFuture<?>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < 50; i++) {
        ScheduledFuture<?> future = JobScheduler.insert(() -> {}, SERVER_RESTART_PERIOD_MS);
        futures.add(future);
        assertThat(future.getDelay(TimeUnit.MILLISECONDS))
            .isLessThanOrEqualTo(SERVER_RESTART_PERIOD_MS);
      }
    } finally {
      futures.forEach(future -> future.cancel(false));
    }
  }

  /** A period too small to jitter in (the 1 ms clamp of insert()) must be handed through as is. */
  @Test
  void tinyPeriodsAreNotJittered() {
    assertThat(JobScheduler.initialDelayWithJitter(1L)).isEqualTo(1L);
    assertThat(JobScheduler.initialDelayWithJitter(9L)).isEqualTo(9L);
    assertThat(JobScheduler.initialDelayWithJitter(10L)).isBetween(10L, 11L);
  }

  /** A period near {@code Long.MAX_VALUE} must not overflow into a negative (= immediate) delay. */
  @Test
  void hugePeriodsAreNotJitteredIntoAnOverflow() {
    assertThat(JobScheduler.initialDelayWithJitter(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
    assertThat(JobScheduler.initialDelayWithJitter(Long.MAX_VALUE - 1)).isPositive();
  }

  /** The jitter has to actually vary — a constant offset would keep every job in lockstep. */
  @Test
  void initialDelayVariesBetweenJobs() {
    Set<Long> seen = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      seen.add(JobScheduler.initialDelayWithJitter(SERVER_RESTART_PERIOD_MS));
    }
    assertThat(seen).hasSizeGreaterThan(1);
  }

  /**
   * TD224, the point of the whole change: {@code SaveJobs} (15 min) and {@code ServerRestartJob}
   * (60 min) used to be inserted with {@code initialDelay == period} within the same millisecond,
   * and 60 is a multiple of 15 — so the hourly restart tick was <b>always</b> also a save tick, for
   * the whole life of the process. That is what made the {@code saveToDisk()}/{@code close()} race
   * of TD221 (REDPANDAJ-2EZ) certain instead of rare: all three testnet restarts of the night of
   * 2026-09-05 landed on a save tick.
   *
   * <p>The test measures the distance between the first restart tick and the nearest save tick.
   * With the jitter it must be a coincidence (expected ≈ 6 of 2000 draws inside a 500 ms window,
   * which is the order of the race window of TD221); without it, it is zero every single time —
   * asserted first, so the test states the old behaviour it replaces.
   */
  @Test
  void jitterBreaksTheStructuralCollisionOfTheHourlyAndTheSaveTick() {
    // the old, unjittered schedule: both initial delays are exactly the period
    assertThat(Math.floorMod(SERVER_RESTART_PERIOD_MS - SAVE_JOBS_PERIOD_MS, SAVE_JOBS_PERIOD_MS))
        .isZero();

    int draws = 2000;
    int collisions = 0;
    for (int i = 0; i < draws; i++) {
      long firstSaveTick = JobScheduler.initialDelayWithJitter(SAVE_JOBS_PERIOD_MS);
      long firstRestartTick = JobScheduler.initialDelayWithJitter(SERVER_RESTART_PERIOD_MS);
      long offset = Math.floorMod(firstRestartTick - firstSaveTick, SAVE_JOBS_PERIOD_MS);
      long distanceToNearestSaveTick = Math.min(offset, SAVE_JOBS_PERIOD_MS - offset);
      if (distanceToNearestSaveTick <= 500L) {
        collisions++;
      }
    }
    // expected ~6, i.e. 0.3% instead of 100%; the bound is ten times that and cannot flake
    assertThat(collisions).isLessThan(60);
  }
}
