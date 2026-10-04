package im.redpanda.ops;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class SettingsKnownNodesTest {

  /**
   * No loopback entry: {@code 127.0.0.1:59558} is the node's own listening address by default, so
   * shipping it as a bootstrap peer made every unconfigured node dial itself (T86). Local setups
   * pass their loopback seeds explicitly.
   */
  private static final String[] DEFAULTS = {"seed1.redpanda.im:59558", "seed2.redpanda.im:59558"};

  @Test
  void defaultsDoNotContainLoopback() {
    for (String defaultNode : Settings.parseKnownNodes(null)) {
      assertFalse(
          im.redpanda.identity.crypt.Utils.isLocalAddress(defaultNode.split(":")[0]),
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

  /**
   * Operator input is trusted and must keep working — the default seed list consists of names
   * (T154a). Lived in {@code InboundCommandProcessorPeerListFilterTest} until T118 moved {@code
   * Settings} into the ops context; it never tested the peer-list filter, only this parser.
   */
  @Test
  void configuredSeedsMayUseHostNames() {
    assertArrayEquals(
        new String[] {"seed1.redpanda.im:59558", "my-host.example:1", "localhost:65535"},
        Settings.parseKnownNodes("seed1.redpanda.im:59558, my-host.example:1,localhost:65535"));
    org.assertj.core.api.Assertions.assertThat(Settings.parseKnownNodes(null))
        .as("the default seeds are DNS names, so a seed can move without a release")
        .containsExactly("seed1.redpanda.im:59558", "seed2.redpanda.im:59558");
  }

  @Test
  void dropsMalformedHostNameEntries() {
    assertArrayEquals(
        new String[] {"seed2.redpanda.im:59558"},
        Settings.parseKnownNodes(
            "host:,:59558,host:abc,host:0,seed1.redpanda.im,seed2.redpanda.im:59558"));
  }

  /**
   * TD144: {@code MIN_CONNECTIONS} is configurable the same way the known nodes are — an invalid
   * value must not silently disable dialling.
   */
  @Test
  void minConnectionsFallsBackToTheDefaultForAnythingUnusable() {
    assertEquals(Settings.DEFAULT_MIN_CONNECTIONS, Settings.parseMinConnections(null));
    assertEquals(Settings.DEFAULT_MIN_CONNECTIONS, Settings.parseMinConnections("  "));
    assertEquals(Settings.DEFAULT_MIN_CONNECTIONS, Settings.parseMinConnections("many"));
    assertEquals(Settings.DEFAULT_MIN_CONNECTIONS, Settings.parseMinConnections("0"));
    assertEquals(Settings.DEFAULT_MIN_CONNECTIONS, Settings.parseMinConnections("-3"));
    assertEquals(3, Settings.parseMinConnections(" 3 "));
  }
}
