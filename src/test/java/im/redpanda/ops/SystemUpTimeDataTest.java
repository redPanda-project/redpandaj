package im.redpanda.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SystemUpTimeDataTest {

  @Test
  void ceilToLastFullHourSimple() {
    long current = System.currentTimeMillis();
    System.out.println(current);
    assertThat(SystemUpTimeData.ceilToLastFullHour(current)).isLessThan(current);
  }

  @Test
  void ceilToLastFullHourSameHour() {
    long timeOne = 1649679240673L;
    long timeFiveMinutesLater = timeOne - 1000 * 60 * 5;
    long timeOneCeil = SystemUpTimeData.ceilToLastFullHour(timeOne);
    long timeFiveMinutesLaterCeil = SystemUpTimeData.ceilToLastFullHour(timeFiveMinutesLater);
    assertThat(timeOneCeil).isEqualTo(timeFiveMinutesLaterCeil);
  }

  @Test
  void uptimeReportNow() {
    SystemUpTimeData systemUpTimeData = new SystemUpTimeData();
    systemUpTimeData.reportNow();
    systemUpTimeData.clearTooOldHits();
    assertThat(systemUpTimeData.getUptimePercent()).isGreaterThan(0d);
  }

  /**
   * REDPANDAJ-2E6: SaveJobs persists this object from the jobs pool while UpTimeReporterJob mutates
   * {@code upHits}. Since T117 the persistence reads {@link SystemUpTimeData#snapshotUpHits()}
   * instead of Java-serializing the object, but the guarantee is the same one: taking the state for
   * a save must not throw while another thread mutates it.
   */
  @Test
  @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
  void snapshotWhileMutating_doesNotThrow() throws Exception {
    TreeSet<Long> hits = new TreeSet<>();
    long oldBase = System.currentTimeMillis() - Duration.ofDays(30).toMillis();
    for (long i = 0; i < 200_000; i++) {
      hits.add(oldBase + i);
    }
    SystemUpTimeData systemUpTimeData = new SystemUpTimeData(hits);

    Thread mutator = new Thread(systemUpTimeData::clearTooOldHits);
    mutator.start();
    try {
      while (mutator.isAlive()) {
        assertThat(systemUpTimeData.snapshotUpHits()).isNotNull();
      }
    } finally {
      mutator.join();
    }
  }

  @Test
  void clearOldData() {
    TreeSet<Long> longs = new TreeSet<>();
    longs.add(1L);
    longs.add(0L);
    SystemUpTimeData systemUpTimeData = new SystemUpTimeData(longs);
    systemUpTimeData.clearTooOldHits();
    assertThat(systemUpTimeData.getUptimePercent()).isEqualTo(0d);
  }
}
