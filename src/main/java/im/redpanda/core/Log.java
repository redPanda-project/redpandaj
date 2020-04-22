/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;

import im.redpanda.jobs.Job;
import io.sentry.Sentry;
import io.sentry.event.Event;
import io.sentry.event.EventBuilder;
import io.sentry.event.helper.EventBuilderHelper;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author robin
 */
public class Log {

    public static int LEVEL = 10;
    private static AtomicInteger rating;

    static {
//        System.out.println("is testing: " + isJUnitTest());
        if (isJUnitTest()) {
            LEVEL = 3000;
//            LEVEL = 0;
        }

        new Job(20000, true) {
            @Override
            public void init() {
                rating = new AtomicInteger();
            }

            @Override
            public void work() {
                int i = rating.decrementAndGet();
                if (i < 0) {
                    rating.set(0);
                }
                System.out.println("current rating for sentry logging: " + i);
            }
        }.start();

    }

    public static void put(String msg, int level) {
        if (level > LEVEL) {
            return;
        }
        System.out.println("Log: " + msg);
    }

    public static void putStd(String msg) {
        if (20 > LEVEL) {
            return;
        }
        System.out.println("Log: " + msg);
    }

    public static void putCritical(Throwable e) {
        if (-200 > LEVEL) {
            return;
        }
        e.printStackTrace();
    }

    public static void sentry(Throwable e) {
        int currentRating = rating.getAndIncrement();
        if (currentRating < 10) {
            try {
                System.out.println("send to Sentry: " + e);
                Sentry.capture(e);
            } catch (Throwable e2) {
                e2.printStackTrace();
            }
        } else {
            rating.decrementAndGet();
        }
    }

    public static void sentry(String msg) {
        int currentRating = rating.getAndIncrement();
        if (currentRating < 10) {
            try {
                System.out.println("send to Sentry: " + msg);
                Sentry.capture(msg);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        } else {
            rating.decrementAndGet();
        }
    }

    public static boolean isJUnitTest() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        List<StackTraceElement> list = Arrays.asList(stackTrace);
        for (StackTraceElement element : list) {
            if (element.getClassName().startsWith("org.junit.")) {
                return true;
            }
        }
        return false;
    }
}
