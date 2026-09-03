package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.ops.Settings;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins the duplicate-address guard of {@link OutboundHandler#run()}.
 *
 * <p>Regression test for the node1&lt;-&gt;node2 reconnect storm on the testnet, 2026-09-03
 * 18:32-18:39 UTC: ~25 redials per minute per node, symmetric, every completed handshake logging
 * "already connected to a node with the same identity" and every second one dying as "duplicate
 * parallel connection".
 *
 * <p>Mechanism: {@link Settings#MIN_CONNECTIONS} is 20, so on a 4-node network {@code actCons}
 * never reaches it and {@code OutboundHandler.run()} never takes its early {@code break} — it hands
 * every dialable peer to the dial logic on every single pass. The duplicate guards are therefore
 * the only thing preventing a permanent redial loop, and this one was inoperative: it tested the
 * <em>candidate's</em> {@code isConnected()/isConnecting} rather than the other peer object's, so a
 * second {@link Peer} for an address we already hold a connection to was dialed anyway. The dial
 * completes, {@link Peer#setupConnectionForPeer} swaps it onto the registered peer (T54 half-open
 * reconnect), the live connection is replaced, and the far side redials in turn.
 */
class OutboundHandlerAddressInUseTest {

  private static Peer peer(String ip, int port) {
    return new Peer(ip, port);
  }

  /**
   * The case that was broken: a <em>different</em> object already holds this address, connected.
   * (The guard looks at every peer carrying the address, the candidate included — see the
   * self-comparison test below.)
   */
  @Test
  void reportsInUse_whenAnotherPeerObjectWithTheSameAddressIsConnected() {
    Peer connected = peer("203.0.113.7", 59558);
    connected.setConnected(true);
    Peer candidate = peer("203.0.113.7", 59558);

    assertThat(OutboundHandler.isAddressAlreadyInUse(List.of(connected, candidate), candidate))
        .isTrue();
  }

  /** Same, while the twin is still mid-dial — dialing again would race it. */
  @Test
  void reportsInUse_whenAnotherPeerObjectWithTheSameAddressIsConnecting() {
    Peer connecting = peer("203.0.113.7", 59558);
    connecting.isConnecting = true;
    Peer candidate = peer("203.0.113.7", 59558);

    assertThat(OutboundHandler.isAddressAlreadyInUse(List.of(connecting, candidate), candidate))
        .isTrue();
  }

  /** Preserved from before the fix: a candidate that is itself already dialing is skipped. */
  @Test
  void reportsInUse_whenTheCandidateItselfIsConnecting() {
    Peer candidate = peer("203.0.113.7", 59558);
    candidate.isConnecting = true;

    assertThat(OutboundHandler.isAddressAlreadyInUse(List.of(candidate), candidate)).isTrue();
  }

  /** A peer we are genuinely not connected to must still be dialable. */
  @Test
  void reportsFree_whenNothingHoldsTheAddress() {
    Peer candidate = peer("203.0.113.7", 59558);
    Peer idleTwin = peer("203.0.113.7", 59558);

    assertThat(OutboundHandler.isAddressAlreadyInUse(List.of(idleTwin, candidate), candidate))
        .isFalse();
  }

  /** A connected peer at a different address must not block this one. */
  @Test
  void reportsFree_whenTheConnectedPeerHasADifferentAddress() {
    Peer otherAddress = peer("203.0.113.8", 59558);
    otherAddress.setConnected(true);
    Peer otherPort = peer("203.0.113.7", 59559);
    otherPort.setConnected(true);
    Peer candidate = peer("203.0.113.7", 59558);

    assertThat(
            OutboundHandler.isAddressAlreadyInUse(
                List.of(otherAddress, otherPort, candidate), candidate))
        .isFalse();
  }

  /** Peers without connection details are allowed in the list and must not throw here. */
  @Test
  void ignoresPeersWithoutAnAddress() {
    Peer withoutAddress = new Peer(null, 0);
    Peer connected = peer("203.0.113.7", 59558);
    connected.setConnected(true);
    Peer candidate = peer("203.0.113.7", 59558);

    assertThat(
            OutboundHandler.isAddressAlreadyInUse(
                List.of(withoutAddress, connected, candidate), candidate))
        .isTrue();
  }
}
