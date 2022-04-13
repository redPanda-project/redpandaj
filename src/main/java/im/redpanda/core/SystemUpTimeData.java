package im.redpanda.core;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.time.Duration;
import java.util.Calendar;
import java.util.SortedSet;
import java.util.TreeSet;

@Slf4j
public class SystemUpTimeData implements Serializable {
    private static final int UPTIME_WINDOW_IN_DAYS = 7;
    private static final int MAX_HITS_IN_WINDOW = UPTIME_WINDOW_IN_DAYS * 24;

    private final SortedSet<Long> upHits;

    public SystemUpTimeData(SortedSet<Long> upHits) {
        this.upHits = upHits;
    }

    public SystemUpTimeData() {
        upHits = new TreeSet<>();
    }

    public void reportNow() {
        clearTooOldHits();
        log.info("current uptime: " + getUptimePercent());
        upHits.add(ceilToLastFullHour(System.currentTimeMillis()));
        log.info("current uptime: " + getUptimePercent() + " after update");
    }

    public void clearTooOldHits() {
        while (!upHits.isEmpty() && upHits.first() < System.currentTimeMillis() - Duration.ofDays(UPTIME_WINDOW_IN_DAYS).toMillis()) {
            upHits.remove(upHits.first());
        }
    }

    public double getUptimePercent() {
        return (double) upHits.size() / MAX_HITS_IN_WINDOW;
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
