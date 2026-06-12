package im.redpanda.flaschenpost;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.Command;
import im.redpanda.core.InboundCommandProcessor;
import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.outbound.OhDht;
import im.redpanda.outbound.OutboundHandleStore;
import im.redpanda.outbound.OutboundMailboxStore;
import im.redpanda.outbound.OutboundService;
import im.redpanda.outbound.v1.MailItem;
import im.redpanda.proto.FlaschenpostPut;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

/**
 * MS04 acceptance: a 3-layer Flaschenpost v2 garlic packet traverses three relays — every relay
 * peels exactly its own layer, rebuilds a fresh 2048-byte packet and forwards it; the last relay
 * deposits the payload into the OH mailbox. Plus the negative paths: dedup, foreign packets,
 * malformed packets.
 */
public class GarlicRouterTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  private ServerContext node1;
  private ServerContext node2;
  private ServerContext node3;
  private InboundCommandProcessor processor1;
  private InboundCommandProcessor processor2;
  private InboundCommandProcessor processor3;
  private OutboundMailboxStore mailbox3;

  private byte[] ohId;

  @Before
  public void setUp() {
    node1 = ServerContext.buildDefaultServerContext();
    node1.setOutboundService(
        new OutboundService(new OutboundHandleStore(), new OutboundMailboxStore()));
    processor1 = new InboundCommandProcessor(node1);

    node2 = ServerContext.buildDefaultServerContext();
    node2.setOutboundService(
        new OutboundService(new OutboundHandleStore(), new OutboundMailboxStore()));
    processor2 = new InboundCommandProcessor(node2);

    node3 = ServerContext.buildDefaultServerContext();
    OutboundHandleStore handleStore3 = new OutboundHandleStore();
    mailbox3 = new OutboundMailboxStore();
    node3.setOutboundService(new OutboundService(handleStore3, mailbox3));
    processor3 = new InboundCommandProcessor(node3);

    ohId = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(ohId);

    // OH registered on node 3 only
    long now = System.currentTimeMillis();
    handleStore3.put(ohId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));
  }

  /** Builds a [4-byte len][payload] frame as expected by {@code parseCommand}. */
  private static ByteBuffer buildFrame(byte[] payload) {
    ByteBuffer buf = ByteBuffer.allocate(4 + payload.length);
    buf.putInt(payload.length);
    buf.put(payload);
    buf.flip();
    return buf;
  }

  /** Adds a connected peer object representing {@code target} to {@code host}'s peer list. */
  private static Peer connect(ServerContext host, ServerContext target, int port) {
    Peer peer = new Peer("127.0.0.1", port, target.getNodeId());
    peer.setConnected(true);
    peer.writeBuffer = ByteBuffer.allocate(65536);
    host.getPeerList().add(peer);
    return peer;
  }

  /** A connected sender peer (the packet origin, e.g. a light client). */
  private static Peer sender(ServerContext host, int port) {
    Peer peer = new Peer("127.0.0.1", port, host.getNodeId());
    peer.setConnected(true);
    peer.writeBuffer = ByteBuffer.allocate(65536);
    host.getPeerList().add(peer);
    return peer;
  }

  /** Reads one FLASCHENPOST_V2 frame from the peer's write buffer. */
  private static byte[] readV2Frame(Peer peer) {
    ByteBuffer out = peer.writeBuffer;
    out.flip();
    assertThat(out.hasRemaining()).as("expected a forwarded FLASCHENPOST_V2 frame").isTrue();
    assertThat(out.get()).isEqualTo(Command.FLASCHENPOST_V2);
    byte[] packet = new byte[out.getInt()];
    out.get(packet);
    assertThat(out.hasRemaining()).as("exactly one frame expected").isFalse();
    out.clear();
    return packet;
  }

  /**
   * Builds a layered garlic packet along the given hops: CMD_FORWARD layers for all but the last
   * hop, a CMD_DELIVER layer (to {@code ohId}) for the last hop.
   */
  private byte[] buildLayeredPacket(byte[] payload, ServerContext... hops) throws Exception {
    ServerContext last = hops[hops.length - 1];
    ByteBuffer deliver = ByteBuffer.allocate(1 + KademliaId.ID_LENGTH_BYTES + 4 + payload.length);
    deliver.put(FlaschenpostV2.CMD_DELIVER);
    deliver.put(ohId);
    deliver.putInt(payload.length);
    deliver.put(payload);
    byte[] body =
        FlaschenpostV2.encryptLayer(
            last.getNodeId().getEncryptionPubKey(), last.getNonce(), deliver.array());

    for (int i = hops.length - 2; i >= 0; i--) {
      ByteBuffer forward = ByteBuffer.allocate(1 + KademliaId.ID_LENGTH_BYTES + body.length);
      forward.put(FlaschenpostV2.CMD_FORWARD);
      forward.put(hops[i + 1].getNonce().getBytes());
      forward.put(body);
      body =
          FlaschenpostV2.encryptLayer(
              hops[i].getNodeId().getEncryptionPubKey(), hops[i].getNonce(), forward.array());
    }
    return FlaschenpostV2.buildPacket(RANDOM.nextInt(), hops[0].getNonce(), body);
  }

  @Test
  public void threeLayerPacket_isPeeledByThreeRelays_andDeliveredToOhMailbox() throws Exception {
    Peer peer1To2 = connect(node1, node2, 9401);
    Peer peer2To3 = connect(node2, node3, 9402);

    byte[] payload = "multi hop garlic message".getBytes(StandardCharsets.UTF_8);
    byte[] packet = buildLayeredPacket(payload, node1, node2, node3);
    int originalPacketId = FlaschenpostV2.parse(packet).getPacketId();

    // relay 1 peels its layer and forwards a rebuilt 2048-byte packet to node 2
    processor1.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(node1, 9400));
    byte[] toNode2 = readV2Frame(peer1To2);
    assertThat(toNode2).hasSize(FlaschenpostV2.PACKET_SIZE);
    FlaschenpostV2 parsed2 = FlaschenpostV2.parse(toNode2);
    assertThat(parsed2.getNextHop()).isEqualTo(node2.getNonce());
    assertThat(parsed2.getPacketId())
        .as("relays must assign a fresh packet_id")
        .isNotEqualTo(originalPacketId);

    // relay 2 peels its layer and forwards to node 3
    processor2.parseCommand(Command.FLASCHENPOST_V2, buildFrame(toNode2), sender(node2, 9403));
    byte[] toNode3 = readV2Frame(peer2To3);
    assertThat(toNode3).hasSize(FlaschenpostV2.PACKET_SIZE);
    assertThat(FlaschenpostV2.parse(toNode3).getNextHop()).isEqualTo(node3.getNonce());

    // relay 3 peels the CMD_DELIVER layer and deposits into the OH mailbox
    processor3.parseCommand(Command.FLASCHENPOST_V2, buildFrame(toNode3), sender(node3, 9404));
    List<MailItem> items = mailbox3.fetchMessages(ohId, 10, 0);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).getPayload().toByteArray()).isEqualTo(payload);
  }

  @Test
  public void duplicatePacketId_isProcessedOnlyOnce() throws Exception {
    Peer peer1To2 = connect(node1, node2, 9411);
    byte[] packet = buildLayeredPacket(new byte[16], node1, node2);

    processor1.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(node1, 9410));
    byte[] first = readV2Frame(peer1To2);
    assertThat(first).hasSize(FlaschenpostV2.PACKET_SIZE);

    // the identical packet (same packet_id) again — must be dropped by the dedup cache
    processor1.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(node1, 9412));
    peer1To2.writeBuffer.flip();
    assertThat(peer1To2.writeBuffer.hasRemaining())
        .as("a duplicate packet_id must not be forwarded again")
        .isFalse();
  }

  @Test
  public void foreignPacket_failsAuthentication_andIsDroppedSilently() throws Exception {
    Peer peer1To2 = connect(node1, node2, 9421);

    // layer encrypted for node 2, but addressed (next_hop) to node 1: node 1 cannot
    // authenticate it and must drop silently — no crash, no forward, no deposit
    byte[] deliver = ByteBuffer.allocate(1 + 20 + 4).put(FlaschenpostV2.CMD_DELIVER).array();
    byte[] body =
        FlaschenpostV2.encryptLayer(
            node2.getNodeId().getEncryptionPubKey(), node2.getNonce(), deliver);
    byte[] packet = FlaschenpostV2.buildPacket(RANDOM.nextInt(), node1.getNonce(), body);

    processor1.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(node1, 9420));

    peer1To2.writeBuffer.flip();
    assertThat(peer1To2.writeBuffer.hasRemaining()).isFalse();
  }

  @Test
  public void packetForAnotherNode_isRoutedUnchangedTowardNextHop() throws Exception {
    Peer peer1To2 = connect(node1, node2, 9431);

    // single-layer packet addressed to node 2 arrives at node 1: pure Kademlia step, the
    // packet must be forwarded byte-identically (no peeling, no rebuild)
    byte[] packet = buildLayeredPacket(new byte[8], node2);

    processor1.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(node1, 9430));

    byte[] forwarded = readV2Frame(peer1To2);
    assertThat(forwarded).isEqualTo(packet);
  }

  @Test
  public void malformedPacket_isDroppedWithoutCrash() {
    Peer peer1To2 = connect(node1, node2, 9441);

    // wrong size — must be dropped before any processing
    processor1.parseCommand(
        Command.FLASCHENPOST_V2, buildFrame(new byte[100]), sender(node1, 9440));

    peer1To2.writeBuffer.flip();
    assertThat(peer1To2.writeBuffer.hasRemaining()).isFalse();
  }

  @Test
  public void deliverForRemoteOh_fallsBackToMs02bForwarding() throws Exception {
    // the final garlic hop (node 1) does not host the OH; it knows the announce record
    // pointing to node 2 and must forward a FlaschenpostPut with the oh_id preserved
    byte[] remoteOhId = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(remoteOhId);
    node1
        .getKadStoreManager()
        .put(OhDht.buildAnnounceContent(remoteOhId, node2.getNonce(), System.currentTimeMillis()));
    Peer peer1To2 = connect(node1, node2, 9451);

    byte[] payload = "deliver elsewhere".getBytes(StandardCharsets.UTF_8);
    ByteBuffer deliver = ByteBuffer.allocate(1 + KademliaId.ID_LENGTH_BYTES + 4 + payload.length);
    deliver.put(FlaschenpostV2.CMD_DELIVER);
    deliver.put(remoteOhId);
    deliver.putInt(payload.length);
    deliver.put(payload);
    byte[] body =
        FlaschenpostV2.encryptLayer(
            node1.getNodeId().getEncryptionPubKey(), node1.getNonce(), deliver.array());
    byte[] packet = FlaschenpostV2.buildPacket(RANDOM.nextInt(), node1.getNonce(), body);

    processor1.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(node1, 9450));

    ByteBuffer out = peer1To2.writeBuffer;
    out.flip();
    assertThat(out.hasRemaining()).as("MS02b forward expected").isTrue();
    assertThat(out.get()).isEqualTo(Command.FLASCHENPOST_PUT);
    byte[] forwardedBytes = new byte[out.getInt()];
    out.get(forwardedBytes);
    FlaschenpostPut forwarded = FlaschenpostPut.parseFrom(forwardedBytes);
    assertThat(forwarded.getOhId().toByteArray()).isEqualTo(remoteOhId);
    assertThat(forwarded.getContent().toByteArray()).isEqualTo(payload);
  }
}
