package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the {@link Log#put(String, int)} / {@link Log#putStd(String)} shim maps the legacy
 * numeric levels onto the intended log4j levels.
 */
class LogShimTest {

  /** Logger name used by {@link Log}'s class-level {@code LogManager.getLogger()}. */
  private static final String LOG_LOGGER_NAME = Log.class.getName();

  private final List<LogEvent> events = new CopyOnWriteArrayList<>();
  private LoggerContext context;
  private CapturingAppender appender;

  private final class CapturingAppender extends AbstractAppender {
    private CapturingAppender() {
      super("LogShimTestAppender", null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }
  }

  @BeforeEach
  void setUp() {
    context = (LoggerContext) LogManager.getContext(false);
    appender = new CapturingAppender();
    appender.start();
    Configuration configuration = context.getConfiguration();
    LoggerConfig loggerConfig = new LoggerConfig(LOG_LOGGER_NAME, Level.ALL, false);
    loggerConfig.addAppender(appender, Level.ALL, null);
    configuration.addLogger(LOG_LOGGER_NAME, loggerConfig);
    context.updateLoggers();
  }

  @AfterEach
  void tearDown() {
    context.getConfiguration().removeLogger(LOG_LOGGER_NAME);
    context.updateLoggers();
    appender.stop();
  }

  private Level levelOfSingleEvent() {
    assertThat(events).hasSize(1);
    return events.get(0).getLevel();
  }

  @Test
  void t86EvictionLevel40MapsToInfo() {
    // PeerJobs eviction messages use legacy level 40 and must be visible on info
    Log.put("removed peer due to inactivity", 40);
    assertThat(levelOfSingleEvent()).isEqualTo(Level.INFO);
  }

  @Test
  void level50BoundaryMapsToInfo() {
    Log.put("boundary", 50);
    assertThat(levelOfSingleEvent()).isEqualTo(Level.INFO);
  }

  @Test
  void level51MapsToDebug() {
    Log.put("just above info boundary", 51);
    assertThat(levelOfSingleEvent()).isEqualTo(Level.DEBUG);
  }

  @Test
  void level150BoundaryMapsToDebug() {
    Log.put("boundary", 150);
    assertThat(levelOfSingleEvent()).isEqualTo(Level.DEBUG);
  }

  @Test
  void level151MapsToTrace() {
    Log.put("per-connection chatter", 151);
    assertThat(levelOfSingleEvent()).isEqualTo(Level.TRACE);
  }

  @Test
  void putStdMapsToInfo() {
    Log.putStd("standard message");
    assertThat(levelOfSingleEvent()).isEqualTo(Level.INFO);
  }

  @Test
  void legacyLevelFieldNoLongerSuppressesOutput() {
    int previous = Log.LEVEL;
    try {
      Log.LEVEL = 10;
      Log.put("suppressed under the legacy gate, visible now", 40);
      assertThat(levelOfSingleEvent()).isEqualTo(Level.INFO);
    } finally {
      Log.LEVEL = previous;
    }
  }
}
