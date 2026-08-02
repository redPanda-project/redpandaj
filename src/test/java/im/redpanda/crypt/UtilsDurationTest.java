package im.redpanda.crypt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class UtilsDurationTest {

  @Test
  void formatDuration_positiveAndNegative() {
    assertEquals("0:00:05", Utils.formatDuration(Duration.ofSeconds(5)));
    assertEquals("-0:00:05", Utils.formatDuration(Duration.ofSeconds(-5)));
    assertEquals("0:01:05", Utils.formatDuration(Duration.ofSeconds(65)));
  }

  @Test
  void formatDurationFromNow_nonEmpty() {
    String s = Utils.formatDurationFromNow(System.currentTimeMillis() - 1500);
    assertTrue(s.endsWith(":01") || s.endsWith(":00"));
  }
}
