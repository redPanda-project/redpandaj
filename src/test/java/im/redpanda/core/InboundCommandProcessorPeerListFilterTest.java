package im.redpanda.core;

import static com.google.protobuf.ByteString.copyFrom;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.identity.NodeId;
import im.redpanda.ops.Settings;
import im.redpanda.proto.NodeIdProto;
import im.redpanda.proto.PeerInfoProto;
import im.redpanda.proto.SendPeerList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the plausibility filter on the peer-list gossip path (T86).
 *
 * <p>The bug: {@code handleSendPeerList} accepted every advertised address, so a node bootstrapped
 * from the testnet seeds ended up with 278 peer-list entries within two minutes, 82 of them {@code
 * 127.0.0.1} and 273 of them with port 0. Since {@code handleRequestPeerList} re-gossips the list
 * verbatim, the entries spread. Peer-list gossip is unauthenticated, so this is an injection vector
 * and not only a cosmetic problem.
 *
 * <p>The filter must stay compatible with the local topologies that really do run on loopback (the
 * mobile e2e suite starts several nodes on {@code 127.0.0.1}, the emulator gate reaches the host at
 * {@code 10.0.2.2}), hence the "who advertises what" rule rather than a blanket ban — the {@code
 * fromLoopbackPeer} cases below are what guards that.
 */
class InboundCommandProcessorPeerListFilterTest {

  private static final String PUBLIC_PEER_IP = "84.147.60.253";
  private static final String LOOPBACK_PEER_IP = "127.0.0.1";

  private ServerContext ctx;
  private InboundCommandProcessor proc;
  private int originalMaxPeerListSize;

  @BeforeEach
  void setup() {
    ctx = ServerContext.buildDefaultServerContext();
    ctx.setPort(59558);
    proc = new InboundCommandProcessor(ctx);
    ByteBufferPool.init();
    originalMaxPeerListSize = Settings.MAX_PEERLIST_SIZE;
  }

  @AfterEach
  void tearDown() {
    Settings.MAX_PEERLIST_SIZE = originalMaxPeerListSize;
  }

  @Test
  void gossipFromPublicPeer_dropsLoopbackPrivateAndPortlessEntries() {
    Peer advertiser = connectedPeer(PUBLIC_PEER_IP, 59558);

    gossip(
        advertiser,
        entry("127.0.0.1", 59558),
        entry("127.0.0.1", 59560),
        entry("10.0.0.9", 59558),
        entry("192.168.1.5", 59558),
        entry("172.20.3.4", 59558),
        entry("169.254.1.1", 59558),
        entry("0.0.0.0", 59558),
        entry("::1", 59558),
        entry(PUBLIC_PEER_IP, 0), // inbound-only peer, not dialable by anyone
        entry("46.224.156.238", 59558)); // the only plausible entry

    assertFalse(contains("127.0.0.1", 59558));
    assertFalse(contains("127.0.0.1", 59560));
    assertFalse(contains("10.0.0.9", 59558));
    assertFalse(contains("192.168.1.5", 59558));
    assertFalse(contains("172.20.3.4", 59558));
    assertFalse(contains("169.254.1.1", 59558));
    assertFalse(contains("0.0.0.0", 59558));
    assertFalse(contains("::1", 59558));
    assertFalse(contains(PUBLIC_PEER_IP, 0));
    assertTrue(contains("46.224.156.238", 59558));
  }

  /** The loopback e2e topology: nodes on 127.0.0.1 must keep discovering each other. */
  @Test
  void gossipFromLoopbackPeer_keepsLoopbackAndPrivateEntries() {
    Peer advertiser = connectedPeer(LOOPBACK_PEER_IP, 59559);

    gossip(
        advertiser,
        entry("127.0.0.1", 59560),
        entry("127.0.0.1", 59561),
        entry("10.0.2.2", 59558), // emulator gate reaches the host here
        entry("192.168.1.5", 59558),
        entry("46.224.156.238", 59558));

    assertTrue(contains("127.0.0.1", 59560));
    assertTrue(contains("127.0.0.1", 59561));
    assertTrue(contains("10.0.2.2", 59558));
    assertTrue(contains("192.168.1.5", 59558));
    assertTrue(contains("46.224.156.238", 59558));
  }

  /**
   * A gossiped host name is resolved by {@code InetSocketAddress} at dial time, so it can point at
   * loopback or the LAN without the string-based locality rule ever seeing it. Rejected on ingest
   * regardless of who sends it — configured seeds are a different, trusted path (see {@link
   * #configuredSeedsMayStillUseHostNames()}).
   */
  @Test
  void gossipedHostNames_areDropped() {
    Peer advertiser = connectedPeer(PUBLIC_PEER_IP, 59558);

    gossip(
        advertiser,
        entry("redpanda.im", 59559),
        entry("evil.example.com", 59558),
        entry("localhost", 59558),
        entry("localtest.me", 59558), // resolves to 127.0.0.1
        entry("46.224.156.238", 59558));

    assertFalse(contains("redpanda.im", 59559));
    assertFalse(contains("evil.example.com", 59558));
    assertFalse(contains("localhost", 59558));
    assertFalse(contains("localtest.me", 59558));
    assertTrue(contains("46.224.156.238", 59558));
  }

  @Test
  void gossipedHostNames_areDroppedFromALoopbackPeerToo() {
    Peer advertiser = connectedPeer(LOOPBACK_PEER_IP, 59559);

    gossip(advertiser, entry("localhost", 59560), entry("127.0.0.1", 59560));

    assertFalse(contains("localhost", 59560), "a name is untrusted no matter who sends it");
    assertTrue(contains("127.0.0.1", 59560), "the literal the e2e topology exchanges still works");
  }

  @Test
  void weDoNotAdvertiseHostNames() {
    ctx.getPeerList().add(connectedPeer("redpanda.im", 59559)); // as reseeding creates it
    ctx.getPeerList().add(connectedPeer("46.224.156.238", 59558));

    assertEquals(List.of("46.224.156.238:59558"), requestPeerListAsSeenBy(PUBLIC_PEER_IP));
  }

  @Test
  void gossipedEntryPointingBackAtUs_isDropped() {
    // even a loopback peer may not make us dial our own listening address
    Peer advertiser = connectedPeer(LOOPBACK_PEER_IP, 59559);

    gossip(advertiser, entry("127.0.0.1", ctx.getPort()), entry("127.0.0.1", ctx.getPort() + 1));

    assertFalse(contains("127.0.0.1", ctx.getPort()));
    assertTrue(contains("127.0.0.1", ctx.getPort() + 1));
  }

  @Test
  void gossipWithIdentity_isFilteredTheSameWay() {
    Peer advertiser = connectedPeer(PUBLIC_PEER_IP, 59558);
    NodeId loopbackNode = NodeId.generateWithSimpleKey();
    NodeId publicNode = NodeId.generateWithSimpleKey();

    gossip(
        advertiser,
        identifiedEntry("127.0.0.1", 59560, loopbackNode),
        identifiedEntry("46.224.156.238", 59558, publicNode));

    assertFalse(ctx.getPeerList().contains(loopbackNode.getKademliaId()));
    assertTrue(ctx.getPeerList().contains(publicNode.getKademliaId()));
  }

  @Test
  void gossipStopsAtThePeerListBound() {
    Peer advertiser = connectedPeer(LOOPBACK_PEER_IP, 59559);
    Settings.MAX_PEERLIST_SIZE = ctx.getPeerList().size() + 3;

    List<PeerInfoProto> entries = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      entries.add(entry("127.0.0.1", 40000 + i));
    }
    gossip(advertiser, entries.toArray(new PeerInfoProto[0]));

    assertEquals(Settings.MAX_PEERLIST_SIZE, ctx.getPeerList().size());
  }

  @Test
  void weDoNotAdvertiseLocalAddressesToAPublicPeer() {
    ctx.getPeerList().add(connectedPeer("127.0.0.1", 59560));
    ctx.getPeerList().add(connectedPeer("192.168.1.5", 59558));
    ctx.getPeerList().add(connectedPeer("46.224.156.238", 59558));
    ctx.getPeerList().add(connectedPeer("5.75.137.166", 0)); // no dialable port

    List<String> advertised = requestPeerListAsSeenBy(PUBLIC_PEER_IP);

    assertEquals(List.of("46.224.156.238:59558"), advertised);
  }

  @Test
  void weStillAdvertiseLocalAddressesToALoopbackPeer() {
    ctx.getPeerList().add(connectedPeer("127.0.0.1", 59560));
    ctx.getPeerList().add(connectedPeer("192.168.1.5", 59558));
    ctx.getPeerList().add(connectedPeer("46.224.156.238", 59558));

    List<String> advertised = requestPeerListAsSeenBy(LOOPBACK_PEER_IP);

    assertTrue(advertised.contains("127.0.0.1:59560"));
    assertTrue(advertised.contains("192.168.1.5:59558"));
    assertTrue(advertised.contains("46.224.156.238:59558"));
  }

  private Peer connectedPeer(String ip, int port) {
    Peer peer = new Peer(ip, port, NodeId.generateWithSimpleKey());
    // no SelectionKey in a unit test, so setWriteBufferFilled() must stay a no-op
    peer.setConnected(false);
    return peer;
  }

  private static PeerInfoProto entry(String ip, int port) {
    return PeerInfoProto.newBuilder().setIp(ip).setPort(port).build();
  }

  private static PeerInfoProto identifiedEntry(String ip, int port, NodeId nodeId) {
    return PeerInfoProto.newBuilder()
        .setIp(ip)
        .setPort(port)
        .setNodeId(
            NodeIdProto.newBuilder().setPublicKeyBytes(copyFrom(nodeId.exportPublic())).build())
        .build();
  }

  private void gossip(Peer advertiser, PeerInfoProto... entries) {
    ctx.getPeerList().add(advertiser);
    byte[] payload = SendPeerList.newBuilder().addAllPeers(List.of(entries)).build().toByteArray();
    ByteBuffer frame = ByteBuffer.allocate(4 + payload.length);
    frame.putInt(payload.length);
    frame.put(payload);
    frame.flip();
    assertEquals(
        1 + 4 + payload.length, proc.parseCommand(Command.SEND_PEERLIST, frame, advertiser));
  }

  /** Runs REQUEST_PEERLIST for a peer at the given address and returns the advertised addresses. */
  private List<String> requestPeerListAsSeenBy(String requesterIp) {
    Peer requester = connectedPeer(requesterIp, 59558);
    ctx.getPeerList().add(requester);
    requester.writeBuffer = ByteBuffer.allocate(1024 * 64);

    assertEquals(1, proc.parseCommand(Command.REQUEST_PEERLIST, ByteBuffer.allocate(0), requester));

    requester.writeBuffer.flip();
    assertEquals(Command.SEND_PEERLIST, requester.writeBuffer.get());
    byte[] payload = new byte[requester.writeBuffer.getInt()];
    requester.writeBuffer.get(payload);

    List<String> advertised = new ArrayList<>();
    try {
      for (PeerInfoProto p : SendPeerList.parseFrom(payload).getPeersList()) {
        if (p.getIp().equals(requesterIp) && p.getPort() == requester.getPort()) {
          continue; // the requester's own entry is not interesting here
        }
        advertised.add(p.getIp() + ":" + p.getPort());
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new AssertionError("peer list frame did not parse", e);
    }
    return advertised;
  }

  private boolean contains(String ip, int port) {
    for (Peer peer : ctx.getPeerList().snapshot()) {
      if (ip.equals(peer.getIp()) && peer.getPort() == port) {
        return true;
      }
    }
    return false;
  }
}
