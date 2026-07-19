package im.redpanda.flaschenpost;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/** T43: token-bucket behaviour of the global channel-record store rate limiter. */
public class RecordStoreRateLimiterTest {

  @Test
  public void admitsUpToCapacityThenDrops() {
    long t0 = 1_000_000L;
    RecordStoreRateLimiter limiter = new RecordStoreRateLimiter(5, 1000L, t0);

    for (int i = 0; i < 5; i++) {
      assertThat(limiter.tryAcquire(t0)).as("store %d within burst", i).isTrue();
    }
    assertThat(limiter.tryAcquire(t0)).as("burst exhausted → drop").isFalse();
  }

  @Test
  public void refillsOverTime() {
    long t0 = 1_000_000L;
    RecordStoreRateLimiter limiter = new RecordStoreRateLimiter(5, 1000L, t0);

    for (int i = 0; i < 5; i++) {
      limiter.tryAcquire(t0);
    }
    assertThat(limiter.tryAcquire(t0)).isFalse();

    // one refill interval later → exactly one token back
    assertThat(limiter.tryAcquire(t0 + 1000L)).isTrue();
    assertThat(limiter.tryAcquire(t0 + 1000L)).isFalse();
  }

  @Test
  public void refillIsCappedAtCapacity() {
    long t0 = 1_000_000L;
    RecordStoreRateLimiter limiter = new RecordStoreRateLimiter(3, 1000L, t0);

    // idle for a long time → tokens cap at capacity, not unbounded
    long later = t0 + 1_000_000L;
    assertThat(limiter.tryAcquire(later)).isTrue();
    assertThat(limiter.tryAcquire(later)).isTrue();
    assertThat(limiter.tryAcquire(later)).isTrue();
    assertThat(limiter.tryAcquire(later)).as("no more than capacity after long idle").isFalse();
  }

  @Test
  public void rejectsNonPositiveConfiguration() {
    long t0 = 1_000_000L;
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> new RecordStoreRateLimiter(0, 1000L, t0))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> new RecordStoreRateLimiter(5, 0L, t0))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
