/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.ops;

import im.redpanda.App;
import im.redpanda.core.ServerContext;
import io.sentry.Sentry;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author robin
 */
public class Log {

  private static final Logger logger = LogManager.getLogger();

  /**
   * Legacy verbosity level, no longer consulted by {@link #put(String, int)} and {@link
   * #putStd(String)} — log output is governed by log4j2.xml. Kept for compatibility (e.g. {@link
   * ListenConsole}).
   */
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

  /**
   * Shim onto log4j: the legacy numeric level is mapped to log4j levels ({@code <= 50} to info,
   * {@code <= 150} to debug, everything else to trace). Filtering is done exclusively by
   * log4j2.xml, the legacy {@link #LEVEL} field is no longer consulted.
   */
  public static void put(String msg, int level) {
    if (level <= 50) {
      logger.info(msg);
    } else if (level <= 150) {
      logger.debug(msg);
    } else {
      logger.trace(msg);
    }
  }

  /** Shim onto log4j: standard messages are logged at info level. */
  public static void putStd(String msg) {
    logger.info(msg);
  }

  public static void putCritical(Throwable e) {
    if (-200 > LEVEL) {
      return;
    }
    e.printStackTrace(); // NOSONAR (java:S4507): controlled console output; full details also
    // sent to logger/Sentry
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
        Sentry.captureException(e);
      } catch (Throwable e2) {
        e2.printStackTrace(); // NOSONAR (java:S4507): last‑resort diagnostics if logging backend
        // fails
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
        Sentry.captureMessage(msg);
      } catch (Throwable e) {
        e.printStackTrace(); // NOSONAR (java:S4507): last‑resort diagnostics if logging backend
        // fails
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
