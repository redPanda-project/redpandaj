package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.ops.Settings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * TD144 — {@code MIN_CONNECTIONS} on a network smaller than {@code MIN_CONNECTIONS}.
 *
 * <p>With the default of 20 a four-node testnet can never reach the minimum, so {@code
 * OutboundHandler.run()} never took its early break and offered every dialable peer to the dial
 * logic on every pass — the standing pressure behind the reconnect storm of 2026-09-03.
 */
class OutboundHandlerMinConnectionsTest {

  private final int originalMinConnections = Settings.MIN_CONNECTIONS;

  @AfterEach
  void restoreSettings() {
    Settings.MIN_CONNECTIONS = originalMinConnections;
  }

  /** The four-node testnet: three dialable peers, all connected — nothing left to dial. */
  @Test
  void aNetworkSmallerThanTheMinimumStopsDiallingOnceEveryPeerIsConnected() {
    Settings.MIN_CONNECTIONS = 20;

    assertThat(OutboundHandler.hasEnoughConnections(3, 3, 3))
        .as("every peer we could dial has a connection, there is nothing left to establish")
        .isTrue();
    assertThat(OutboundHandler.hasEnoughConnections(2, 2, 3))
        .as("one dialable peer is still unconnected, the pass must go on")
        .isFalse();
  }

  /** Inbound-only peers (light clients announce port 0) must not satisfy the minimum. */
  @Test
  void lightClientsDoNotCountTowardsTheDialMinimum() {
    Settings.MIN_CONNECTIONS = 20;

    // 8 connections, but 5 of them are light clients and one of the three nodes is missing
    assertThat(OutboundHandler.hasEnoughConnections(7, 2, 3)).isFalse();
  }

  /** A network large enough to reach the minimum behaves exactly as before. */
  @Test
  void aLargeNetworkStillUsesTheConfiguredMinimum() {
    Settings.MIN_CONNECTIONS = 20;

    assertThat(OutboundHandler.hasEnoughConnections(20, 20, 100)).isTrue();
    assertThat(OutboundHandler.hasEnoughConnections(19, 19, 100))
        .as("the cap must never end a pass early while dialable peers are left")
        .isFalse();
  }

  /** Nothing dialable at all: there is nothing this loop could do. */
  @Test
  void withoutAnyDialablePeerThePassEndsImmediately() {
    Settings.MIN_CONNECTIONS = 20;

    assertThat(OutboundHandler.hasEnoughConnections(4, 0, 0)).isTrue();
  }

  /** The minimum itself is configurable, so a deployment can lower it without a rebuild. */
  @Test
  void theMinimumIsConfigurable() {
    Settings.MIN_CONNECTIONS = 2;

    assertThat(OutboundHandler.hasEnoughConnections(2, 2, 50)).isTrue();
    assertThat(OutboundHandler.hasEnoughConnections(1, 1, 50)).isFalse();
  }
}
