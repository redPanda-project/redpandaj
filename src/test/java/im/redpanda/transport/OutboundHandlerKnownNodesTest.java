package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import im.redpanda.identity.crypt.Utils;
import java.net.UnknownHostException;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Configured seeds given as DNS names (T154a): {@code seed1.redpanda.im:59558} has to end up as the
 * same peer-list entry as the node behind it, however that node reaches us otherwise.
 *
 * <p>The peer list identifies an address by its string. A seed kept as a name never matches the
 * same node known by its IP — from its inbound connection, from peer-list gossip, from disk — so
 * each reseed would add an id-less duplicate whose dial ends in a handshake that merges into the
 * owner by identity and replaces its live connection. {@link OutboundHandler#addKnownNodes}
 * therefore resolves names before they enter the peer list; these tests pin that and the merges it
 * enables.
 */
class OutboundHandlerKnownNodesTest {

  private static final String SEED1 = "seed1.redpanda.im";
  private static final String SEED1_IP = "91.98.79.117";
  private static final String SEED2 = "seed2.redpanda.im";
  private static final String SEED2_IP = "5.75.137.166";
  private static final int PORT = 59558;

  private static final String[] KNOWN_NODES = {SEED1 + ":" + PORT, SEED2 + ":" + PORT};

  /** Stands in for DNS so the tests neither need network access nor the real records. */
  private static final OutboundHandler.HostResolver FAKE_DNS =
      host -> {
        String ip = Map.of(SEED1, SEED1_IP, SEED2, SEED2_IP).get(host);
        if (ip == null) {
          throw new UnknownHostException(host);
        }
        return ip;
      };

  private final PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();

  @Test
  void hostNamesEnterThePeerListAsIpLiterals() {
    OutboundHandler.addKnownNodes(peerList, KNOWN_NODES, FAKE_DNS);

    assertThat(peerList.snapshot())
        .extracting(Peer::getIp)
        .containsExactlyInAnyOrder(SEED1_IP, SEED2_IP);
    assertThat(peerList.getByAddress(SEED1_IP, PORT)).isNotNull();
    assertThat(peerList.getByAddress(SEED1, PORT)).as("no entry keyed by the name").isNull();
  }

  @Test
  void anUnresolvableSeedIsSkippedAndTheOthersAreStillAdded() {
    OutboundHandler.addKnownNodes(
        peerList, new String[] {"gone.redpanda.im:" + PORT, SEED2 + ":" + PORT}, FAKE_DNS);

    assertThat(peerList.snapshot()).extracting(Peer::getIp).containsExactly(SEED2_IP);
  }

  /**
   * The seed node dialled us (or was gossiped, or restored from disk) before the reseed ran: the
   * reseeded entry has to collapse onto the identified owner instead of becoming a second dial
   * candidate for the same socket.
   */
  @Test
  void reseedingANodeWeAlreadyKnowByAddressAddsNoSecondObject() {
    NodeId identity = NodeId.generateWithSimpleKey();
    Peer owner = new Peer(SEED1_IP, PORT, identity);
    peerList.add(owner);

    OutboundHandler.addKnownNodes(peerList, new String[] {SEED1 + ":" + PORT}, FAKE_DNS);
    OutboundHandler.addKnownNodes(peerList, new String[] {SEED1 + ":" + PORT}, FAKE_DNS);

    assertThat(peerList.snapshot()).containsExactly(owner);
    assertThat(peerList.getByAddress(SEED1_IP, PORT)).isSameAs(owner);
  }

  /**
   * The other order: the reseeded placeholder is in the list first and the node then identifies
   * itself — through the handshake of our own dial ({@code updateKademliaId}), through its inbound
   * connection, and through peer-list gossip naming its IP. All three must end on one object.
   */
  @Test
  void aReseededPlaceholderMergesWithTheIdentityFromHandshakeInboundAndGossip() {
    OutboundHandler.addKnownNodes(peerList, new String[] {SEED1 + ":" + PORT}, FAKE_DNS);
    Peer placeholder = peerList.getByAddress(SEED1_IP, PORT);
    assertThat(placeholder.getNodeId()).isNull();

    NodeId identity = NodeId.generateWithSimpleKey();

    // our dial's handshake announces the identity (ConnectionReaderThread.parseHandshake)
    assertThat(peerList.updateKademliaId(placeholder, identity.getKademliaId()))
        .isSameAs(placeholder);
    // the node's own inbound connection, as ConnectionReaderThread/setupConnection builds it
    Peer inbound = new Peer(SEED1_IP, PORT, new NodeId(identity.getKademliaId()));
    assertThat(peerList.add(inbound)).isSameAs(placeholder);
    // a third node gossips it (PeerExchangeHandler.handleSendPeerList)
    assertThat(peerList.add(new Peer(SEED1_IP, PORT, identity))).isSameAs(placeholder);
    // and the next reseed
    OutboundHandler.addKnownNodes(peerList, new String[] {SEED1 + ":" + PORT}, FAKE_DNS);

    assertThat(peerList.snapshot()).containsExactly(placeholder);
    assertThat(peerList.get(identity.getKademliaId())).isSameAs(placeholder);
    assertThat(placeholder.getIp())
        .as("an IP literal, so the seed is also advertised to others")
        .isEqualTo(SEED1_IP);
    assertThat(Utils.isPlausibleAdvertisedAddress(placeholder.getIp(), PORT, "84.147.60.253"))
        .isTrue();
  }

  /**
   * Why the resolution is needed at all: the peer list does not see through a name. An entry
   * carrying the name sits next to the owner of the IP as a second dial candidate for the same node
   * — what every reseed produced before T154a — and its address could never be advertised.
   */
  @Test
  void withoutResolutionTheNameWouldBeASecondEntryForTheSameNode() {
    Peer owner = new Peer(SEED1_IP, PORT, NodeId.generateWithSimpleKey());
    peerList.add(owner);

    peerList.add(new Peer(SEED1, PORT));

    assertThat(peerList.snapshot()).hasSize(2);
    assertThat(Utils.isPlausibleAdvertisedAddress(SEED1, PORT, "84.147.60.253")).isFalse();
  }

  /** The production resolver: literals pass through untouched, names come back as literals. */
  @Test
  void theDnsResolverReturnsIpLiterals() throws UnknownHostException {
    assertThat(OutboundHandler.DNS.resolve(SEED2_IP)).isEqualTo(SEED2_IP);
    String localhost = OutboundHandler.DNS.resolve("localhost");
    assertThat(Utils.isIpLiteral(localhost)).as(localhost).isTrue();
    assertThat(Utils.isLocalAddress(localhost)).as(localhost).isTrue();
  }
}
