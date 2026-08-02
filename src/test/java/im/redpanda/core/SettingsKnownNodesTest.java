package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class SettingsKnownNodesTest {

  /**
   * No loopback entry: {@code 127.0.0.1:59558} is the node's own listening address by default, so
   * shipping it as a bootstrap peer made every unconfigured node dial itself (T86). Local setups
   * pass their loopback seeds explicitly.
   */
  private static final String[] DEFAULTS = {"195.201.25.223:59558", "redpanda.im:59559"};

  @Test
  void defaultsDoNotContainLoopback() {
    for (String defaultNode : Settings.parseKnownNodes(null)) {
      assertFalse(
          im.redpanda.crypt.Utils.isLocalAddress(defaultNode.split(":")[0]),
          "loopback must not be a default bootstrap peer: " + defaultNode);
    }
  }

  @Test
  void nullFallsBackToDefaults() {
    assertArrayEquals(DEFAULTS, Settings.parseKnownNodes(null));
  }

  @Test
  void blankFallsBackToDefaults() {
    assertArrayEquals(DEFAULTS, Settings.parseKnownNodes("   "));
    assertArrayEquals(DEFAULTS, Settings.parseKnownNodes(",,"));
  }

  @Test
  void parsesCommaSeparatedListAndTrims() {
    assertArrayEquals(
        new String[] {"5.75.137.166:59558", "46.224.156.238:59558"},
        Settings.parseKnownNodes(" 5.75.137.166:59558 , 46.224.156.238:59558 "));
  }

  @Test
  void dropsEmptyEntries() {
    assertArrayEquals(
        new String[] {"node.example.org:59558"},
        Settings.parseKnownNodes("node.example.org:59558,, "));
  }

  @Test
  void dropsInvalidEntriesButKeepsValidOnes() {
    assertArrayEquals(
        new String[] {"5.75.137.166:59558"},
        Settings.parseKnownNodes(
            "no-port,host:notaport,host:0,host:70000,:59558,a:b:c,5.75.137.166:59558"));
  }

  @Test
  void allInvalidFallsBackToDefaults() {
    assertArrayEquals(DEFAULTS, Settings.parseKnownNodes("no-port,host:notaport"));
  }

  @Test
  void noneDisablesBootstrapping() {
    assertArrayEquals(new String[0], Settings.parseKnownNodes("none"));
    assertArrayEquals(new String[0], Settings.parseKnownNodes(" NONE "));
  }

  @Test
  void acceptsBracketedIpv6() {
    assertArrayEquals(new String[] {"[2001:db8::1]"}, Settings.parseKnownNodes("[2001:db8::1]"));
  }
}
