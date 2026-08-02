package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ObjectOutputStream;
import java.io.OutputStream;
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

  @Test
  @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
  void serializeWhileMutating_doesNotThrow() throws Exception {
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
        try (ObjectOutputStream out = new ObjectOutputStream(OutputStream.nullOutputStream())) {
          out.writeObject(systemUpTimeData);
        }
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
