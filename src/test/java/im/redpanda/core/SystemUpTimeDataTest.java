package im.redpanda.core;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.TreeSet;

import static org.hamcrest.MatcherAssert.assertThat;


public class SystemUpTimeDataTest {


    @Test
    public void testCeilToLastFullHour_simple() {
        long current = System.currentTimeMillis();
        System.out.println(current);
        assertThat(SystemUpTimeData.ceilToLastFullHour(current), Matchers.lessThan(current));
    }

    @Test
    public void testCeilToLastFullHour_sameHour() {
        long timeOne = 1649679240673L;
        long timeFiveMinutesLater = timeOne - 1000 * 60 * 5;
        long timeOneCeil = SystemUpTimeData.ceilToLastFullHour(timeOne);
        long timeFiveMinutesLaterCeil = SystemUpTimeData.ceilToLastFullHour(timeFiveMinutesLater);
        assertThat(timeOneCeil, Matchers.is(timeFiveMinutesLaterCeil));
    }


    @Test
    public void uptimeReportNow() {
        SystemUpTimeData systemUpTimeData = new SystemUpTimeData();
        systemUpTimeData.reportNow();
        systemUpTimeData.clearTooOldHits();
        assertThat(systemUpTimeData.getUptimePercent(), Matchers.greaterThan(0d));
    }

    @Test
    public void clearOldData() {
        TreeSet<Long> longs = new TreeSet<>();
        longs.add(1L);
        longs.add(0L);
        SystemUpTimeData systemUpTimeData = new SystemUpTimeData(longs);
        systemUpTimeData.clearTooOldHits();
        assertThat(systemUpTimeData.getUptimePercent(), Matchers.is(0d));
    }


}