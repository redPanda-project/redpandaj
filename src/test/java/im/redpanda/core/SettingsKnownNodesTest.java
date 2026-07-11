package im.redpanda.core;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

public class SettingsKnownNodesTest {

  private static final String[] DEFAULTS = {
    "195.201.25.223:59558", "redpanda.im:59559", "127.0.0.1:59558"
  };

  @Test
  public void nullFallsBackToDefaults() {
    assertArrayEquals(DEFAULTS, Settings.parseKnownNodes(null));
  }

  @Test
  public void blankFallsBackToDefaults() {
    assertArrayEquals(DEFAULTS, Settings.parseKnownNodes("   "));
    assertArrayEquals(DEFAULTS, Settings.parseKnownNodes(",,"));
  }

  @Test
  public void parsesCommaSeparatedListAndTrims() {
    assertArrayEquals(
        new String[] {"5.75.137.166:59558", "46.224.156.238:59558"},
        Settings.parseKnownNodes(" 5.75.137.166:59558 , 46.224.156.238:59558 "));
  }

  @Test
  public void dropsEmptyEntries() {
    assertArrayEquals(
        new String[] {"node.example.org:59558"},
        Settings.parseKnownNodes("node.example.org:59558,, "));
  }
}
