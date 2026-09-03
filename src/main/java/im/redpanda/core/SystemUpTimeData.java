package im.redpanda.core;

import java.time.Duration;
import java.util.Calendar;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SystemUpTimeData {
  private static final int UPTIME_WINDOW_IN_DAYS = 7;
  private static final int MAX_HITS_IN_WINDOW = UPTIME_WINDOW_IN_DAYS * 24;

  private final SortedSet<Long> upHits;

  public SystemUpTimeData(SortedSet<Long> upHits) {
    this.upHits = upHits;
  }

  public SystemUpTimeData() {
    upHits = new TreeSet<>();
  }

  public synchronized void reportNow() {
    clearTooOldHits();
    log.info("current uptime: " + getUptimePercent());
    upHits.add(ceilToLastFullHour(System.currentTimeMillis()));
    log.info("current uptime: " + getUptimePercent() + " after update");
  }

  public synchronized void clearTooOldHits() {
    while (!upHits.isEmpty()
        && upHits.getFirst()
            < System.currentTimeMillis() - Duration.ofDays(UPTIME_WINDOW_IN_DAYS).toMillis()) {
      upHits.remove(upHits.getFirst());
    }
  }

  public synchronized double getUptimePercent() {
    return (double) upHits.size() / MAX_HITS_IN_WINDOW;
  }

  /**
   * A copy of the recorded hourly up-hits, for persistence.
   *
   * <p>Synchronized for the same reason the removed {@code writeObject} was: SaveJobs persists this
   * object from the jobs pool while UpTimeReporterJob updates {@code upHits} concurrently
   * (REDPANDAJ-2E6). A copy, so the caller can serialize it outside the lock.
   */
  public synchronized SortedSet<Long> snapshotUpHits() {
    return new TreeSet<>(upHits);
  }

  public int getUptimePercentAsInt() {
    return (int) Math.round(100d * getUptimePercent());
  }

  public static long ceilToLastFullHour(long millis) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(millis);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    return calendar.getTimeInMillis();
  }
}
