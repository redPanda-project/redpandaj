package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import org.junit.jupiter.api.Test;

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
 *
 * <p>The second half (TD027, tests at the bottom) is what the map was keyed by: {@code
 * ip.hashCode() + port} is a hash, not a key, so two <em>different</em> addresses could share a
 * slot. The key is the address itself now, and the last removal path that still removed by key
 * instead of by peer ({@code removeIpPortOnly}, used by {@code clearConnectionDetails}) is
 * value-checked like the other two.
 */
class PeerListIpPortConsistencyTest {

  /** An inbound light client as {@code ConnectionHandler.setupConnection} builds it: port 0. */
  private static Peer inboundLightClient(String ip, NodeId identity) {
    Peer peer = new Peer(ip, 0);
    peer.setNodeId(identity);
    return peer;
  }

  @Test
  void removingAnUndialablePeerAlsoDropsItsIpPortEntry() {
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
  void removingAnUndialablePeerWithoutIdentityAlsoDropsItsIpPortEntry() {
    PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();

    Peer anonymous = new Peer("127.0.0.1", 0);
    peerList.add(anonymous);

    assertThat(peerList.remove(anonymous)).isTrue();

    assertThat(peerList.removeIpPort("127.0.0.1", 0))
        .as("removeByObject must take the ip+port entry with it")
        .isFalse();
  }

  /**
   * Every inbound light client from the same ip announces port 0, so they all share the key {@code
   * "127.0.0.1:0"} and the last {@code add} owns the mapping. Removing an earlier peer must not
   * evict the mapping of the peer that owns it now — which is why the removal is value-checked
   * rather than a plain {@code remove(key)}.
   */
  @Test
  void removalDoesNotStealAColocatedPeersIpPortEntry() {
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
  void removingADialablePeerStillDropsItsIpPortEntry() {
    PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();

    NodeId identity = NodeId.generateWithSimpleKey();
    Peer node = new Peer("46.224.156.238", 59558, identity);
    peerList.add(node);

    assertThat(peerList.remove(node)).isTrue();

    Peer reconnect = new Peer("46.224.156.238", 59558, new NodeId(identity.getKademliaId()));
    assertThat(peerList.add(reconnect)).isNull();
    assertThat(peerList.get(identity.getKademliaId())).isSameAs(reconnect);
  }

  // ---------------------------------------------------------------------------------------------
  // TD027: the ip+port map used to be keyed by ip.hashCode() + port — a hash, not a key.
  // ---------------------------------------------------------------------------------------------

  /**
   * Two addresses that collided under the old {@code ip.hashCode() + port} key.
   *
   * <p>{@code "10.0.0.11".hashCode()} and {@code "10.0.0.21".hashCode()} differ by exactly 31 (one
   * character, one position from the end), so the two addresses below produced the same {@code
   * int}. {@link String#hashCode()} is specified, so this pair is stable forever; {@link
   * #theTwoAddressesReallyCollideUnderTheOldHashKey} asserts it rather than trusting the comment.
   */
  private static final String COLLIDING_IP_A = "10.0.0.11";

  private static final int COLLIDING_PORT_A = 59558;
  private static final String COLLIDING_IP_B = "10.0.0.21";
  private static final int COLLIDING_PORT_B = 59527;

  /** Guards the premise of the two tests below: without a collision they would prove nothing. */
  @Test
  void theTwoAddressesReallyCollideUnderTheOldHashKey() {
    assertThat(COLLIDING_IP_A.hashCode() + COLLIDING_PORT_A)
        .as("the old key was ip.hashCode() + port and these two addresses shared it")
        .isEqualTo(COLLIDING_IP_B.hashCode() + COLLIDING_PORT_B);
    assertThat(COLLIDING_IP_A).isNotEqualTo(COLLIDING_IP_B);
  }

  /**
   * TD027(a): two peers at different addresses that hashed to the same old key must both keep their
   * own mapping.
   *
   * <p>Both are ordinary dialable nodes, the kind a gossiped peer list carries — and since a peer
   * announces its own ip and port there, the colliding entry was remote-controllable. Under the old
   * key, adding {@code b} silently took over {@code a}'s slot, so the map answered every question
   * about {@code a}'s address with {@code b}: {@code add()} refused to register a new peer for
   * {@code a}'s address (it returned {@code b} as the pre-existing one), and {@code removeIpPort}
   * on {@code a}'s address cascaded a full removal onto {@code b}, a live peer at an entirely
   * different address.
   */
  @Test
  void twoPeersWithCollidingAddressHashesKeepTheirOwnMapping() {
    PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();

    Peer a = new Peer(COLLIDING_IP_A, COLLIDING_PORT_A, NodeId.generateWithSimpleKey());
    Peer b = new Peer(COLLIDING_IP_B, COLLIDING_PORT_B, NodeId.generateWithSimpleKey());
    peerList.add(a);
    peerList.add(b);

    // add() of a peer without an identity resolves purely through the address map and returns
    // whoever is registered at that address, without registering the newcomer. It must be the peer
    // that actually lives there.
    assertThat(peerList.add(new Peer(COLLIDING_IP_A, COLLIDING_PORT_A)))
        .as("a's address must resolve to a, not to the peer that merely hashed to the same key")
        .isSameAs(a);

    // removeIpPort cascades: it drops whatever the address map points at from all three
    // structures. Removing a must therefore leave b completely untouched.
    assertThat(peerList.removeIpPort(COLLIDING_IP_A, COLLIDING_PORT_A)).isTrue();
    assertThat(peerList.get(a.getKademliaId())).isNull();
    assertThat(peerList.get(b.getKademliaId()))
        .as("removing a must not evict b, which only shared the old hash key")
        .isSameAs(b);
    assertThat(peerList.removeIpPort(COLLIDING_IP_B, COLLIDING_PORT_B))
        .as("b must still be reachable through its own address")
        .isTrue();
  }

  /**
   * TD027(b): {@code clearConnectionDetails} — the "the client wiped its data" path in {@code
   * ConnectionReaderThread:230} — must not evict a live neighbour's mapping.
   *
   * <p>T88 made the two peer-based removal paths value-checked but left {@code removeIpPortOnly} on
   * an unconditional {@code remove(key)}. Alice and Bob are two inbound light clients from the same
   * ip, so they genuinely share the key {@code "127.0.0.1:0"} and Bob, added last, owns it. Wiping
   * Alice's connection details used to drop Bob's mapping instead.
   */
  @Test
  void clearingConnectionDetailsDoesNotStealAColocatedPeersIpPortEntry() {
    PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();

    Peer alice = inboundLightClient("127.0.0.1", NodeId.generateWithSimpleKey());
    Peer bob = inboundLightClient("127.0.0.1", NodeId.generateWithSimpleKey());
    peerList.add(alice);
    peerList.add(bob); // same address: bob now owns the mapping

    peerList.clearConnectionDetails(alice);

    assertThat(alice.getIp()).as("alice's own details are still cleared").isNull();
    // removeIpPort resolves through the address map and cascades to whoever it points at — so if it
    // still finds and drops bob, alice's clearConnectionDetails left his entry alone.
    assertThat(peerList.removeIpPort("127.0.0.1", 0))
        .as("bob's ip+port mapping must survive alice's clearConnectionDetails")
        .isTrue();
    assertThat(peerList.get(bob.getKademliaId())).isNull();
  }

  /** {@code removeIpPortOnly} reports whether it removed <em>this</em> peer's mapping. */
  @Test
  void removeIpPortOnlyOnlyRemovesTheMappingThePeerOwns() {
    PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();

    Peer alice = inboundLightClient("127.0.0.1", NodeId.generateWithSimpleKey());
    Peer bob = inboundLightClient("127.0.0.1", NodeId.generateWithSimpleKey());
    peerList.add(alice);
    peerList.add(bob);

    assertThat(peerList.removeIpPortOnly(alice))
        .as("alice never owned the mapping, so there is nothing for her to remove")
        .isFalse();
    assertThat(peerList.removeIpPortOnly(bob)).as("bob owns it").isTrue();
    assertThat(peerList.removeIpPortOnly(bob)).as("and only once").isFalse();
    assertThat(peerList.size()).as("both peers are still in the list itself").isEqualTo(2);
  }

  /**
   * A peer without connection details is explicitly allowed in the peer list (see {@code
   * addLocked}'s javadoc), and there is no address to key it by — so it must simply stay out of the
   * address map instead of landing in a shared {@code "null:0"} bucket.
   */
  @Test
  void peersWithoutConnectionDetailsStayOutOfTheAddressMap() {
    PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();

    Peer wiped = inboundLightClient("127.0.0.1", NodeId.generateWithSimpleKey());
    peerList.add(wiped);
    peerList.clearConnectionDetails(wiped);

    // Before the composite key this threw an NPE inside addPeer (ip.hashCode() on a null ip); a
    // "null:0" key would be no better, it would make every address-less peer collide with every
    // other one.
    Peer alsoWiped = new Peer(null, 0, NodeId.generateWithSimpleKey());
    assertThat(peerList.add(alsoWiped))
        .as("an address-less peer is registered on its identity alone")
        .isNull();
    assertThat(peerList.get(alsoWiped.getKademliaId())).isSameAs(alsoWiped);
    assertThat(peerList.get(wiped.getKademliaId())).isSameAs(wiped);
  }
}
