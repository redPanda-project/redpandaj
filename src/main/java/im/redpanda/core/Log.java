/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;

import im.redpanda.App;
import im.redpanda.jobs.Job;
import io.sentry.Sentry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author robin
 */
public class Log {

    private static final Logger logger = LogManager.getLogger();

    public static int LEVEL = 10;
    private static AtomicInteger rating;


    public static void init(ServerContext serverContext) {
        //        System.out.println("is testing: " + isJUnitTest());
        if (isJUnitTest()) {
            LEVEL = 3000;
//            LEVEL = 0;
        }

        new Job(serverContext, 20000, true) {
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
//                logger.trace("current rating for sentry logging: " + i);
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
        if (!App.sentryAllowed) {
            logger.warn(e);
            return;
        }
        int currentRating = rating.getAndIncrement();
        if (currentRating < 10) {
            try {
                logger.error("send to Sentry: " + e);
                Sentry.capture(e);
            } catch (Throwable e2) {
                e2.printStackTrace();
                logger.error(e2);
            }
        } else {
            int i = rating.decrementAndGet();
            logger.warn("could not log to sentry because of throttling... " + i);
        }
    }

    public static void sentry(String msg) {
        if (!App.sentryAllowed) {
            return;
        }
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
        StackTraceElement[] list = stackTrace;
        for (StackTraceElement element : list) {
            if (element.getClassName().startsWith("org.junit.")) {
                return true;
            }
        }
        return false;
    }
}
