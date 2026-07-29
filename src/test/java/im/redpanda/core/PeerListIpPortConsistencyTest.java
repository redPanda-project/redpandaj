package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * Pins the ip+port map against the {@link KademliaId} map (T88).
 *
 * <p>{@code PeerList.addPeer} inserts every peer into <em>both</em> maps, but the two removal paths
 * used to skip {@code peerlistIpPort} whenever {@code port == 0} — i.e. for exactly the entries an
 * inbound light client creates, because the handshake carries the sender's listening port and a
 * light client has none ({@code ConnectionReaderThread:151}). The removed peer therefore stayed
 * reachable through the ip+port branch of {@code add()} while its identity key was gone, and that
 * ghost was handed back as {@code oldPeer} for the next connection of the same identity, so the
 * reconnecting peer was never registered and {@code ConnectionHandler.setupConnection} dropped it
 * as a TD020 duplicate — on every retry, with no way out.
 *
 * <p>The path only became reachable with T86/#294, which is the first code that ever removes an
 * undialable (port-0) peer; before that they simply leaked. It surfaced as the S4 airplane-mode
 * gate failing with 154 consecutive "duplicate parallel connection from the same identity" lines.
 */
public class PeerListIpPortConsistencyTest {

  /** An inbound light client as {@code ConnectionHandler.setupConnection} builds it: port 0. */
  private static Peer inboundLightClient(String ip, NodeId identity) {
    Peer peer = new Peer(ip, 0);
    peer.setNodeId(identity);
    return peer;
  }

  @Test
  public void removingAnUndialablePeerAlsoDropsItsIpPortEntry() {
    PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();

    NodeId identity = NodeId.generateWithSimpleKey();
    Peer first = inboundLightClient("127.0.0.1", identity);
    peerList.add(first);

    assertThat(peerList.remove(first)).isTrue();

    // The reconnect: parseHandshake found nothing for this identity and built a fresh Peer.
    Peer reconnect = inboundLightClient("127.0.0.1", new NodeId(identity.getKademliaId()));

    assertThat(peerList.add(reconnect))
        .as("a removed peer must not come back as oldPeer through the ip+port map")
        .isNull();
    assertThat(peerList.get(identity.getKademliaId()))
        .as("the reconnecting peer must be the registered one")
        .isSameAs(reconnect);
  }

  /**
   * The same for {@code removeByObject}, the path a peer without a {@link KademliaId} takes. {@code
   * removeIpPort} is the observation: it resolves through the ip+port map, so it can only still
   * find something if the removal left the entry behind.
   */
  @Test
  public void removingAnUndialablePeerWithoutIdentityAlsoDropsItsIpPortEntry() {
    PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();

    Peer anonymous = new Peer("127.0.0.1", 0);
    peerList.add(anonymous);

    assertThat(peerList.remove(anonymous)).isTrue();

    assertThat(peerList.removeIpPort("127.0.0.1", 0))
        .as("removeByObject must take the ip+port entry with it")
        .isFalse();
  }

  /**
   * {@code getIpPortHash} is {@code ip.hashCode() + port}, so every loopback light client shares
   * one key and the last {@code add} owns the mapping. Removing an earlier peer must not evict the
   * mapping of the peer that owns it now — which is why the removal is value-checked rather than a
   * plain {@code remove(key)}.
   */
  @Test
  public void removalDoesNotStealAColocatedPeersIpPortEntry() {
    PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();

    Peer alice = inboundLightClient("127.0.0.1", NodeId.generateWithSimpleKey());
    Peer bob = inboundLightClient("127.0.0.1", NodeId.generateWithSimpleKey());
    peerList.add(alice);
    peerList.add(bob); // same ip+port hash: bob now owns the mapping

    peerList.remove(alice);

    // removeIpPort resolves through that mapping and cascades to whoever it points at — so if it
    // still finds and drops bob, alice's removal left his entry alone.
    assertThat(peerList.removeIpPort("127.0.0.1", 0))
        .as("bob's ip+port mapping must survive alice's removal")
        .isTrue();
    assertThat(peerList.get(bob.getKademliaId())).isNull();
  }

  /** A dialable peer keeps behaving as before — the guard removal must not change that. */
  @Test
  public void removingADialablePeerStillDropsItsIpPortEntry() {
    PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();

    NodeId identity = NodeId.generateWithSimpleKey();
    Peer node = new Peer("46.224.156.238", 59558, identity);
    peerList.add(node);

    assertThat(peerList.remove(node)).isTrue();

    Peer reconnect = new Peer("46.224.156.238", 59558, new NodeId(identity.getKademliaId()));
    assertThat(peerList.add(reconnect)).isNull();
    assertThat(peerList.get(identity.getKademliaId())).isSameAs(reconnect);
  }
}
