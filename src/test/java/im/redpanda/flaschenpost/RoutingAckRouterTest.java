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
import im.redpanda.outbound.v1.RoutingAck;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

/**
 * MS06 acceptance: a {@code CMD_DELIVER_ACKED} deliver deposits like MS04/MS05 and the node with
 * the final deposit decision sends a {@code RoutingAck} back through the sender-chosen return-path
 * hops — a standard MS04 onion whose innermost {@code CMD_DELIVER_TAGGED} layer lands in the
 * sender's own OH mailbox under the ack session tag.
 *
 * <p>Plus the negative paths: malformed tag length / hop count / truncated return paths are dropped
 * silently, an invalid {@code FlaschenpostPut.return_path} rejects the deposit, and the MS02b
 * fallback conserves the return path so the OH host node acks (including the handle_expired R-ACK
 * when the hop budget is exhausted at a non-host).
 */
public class RoutingAckRouterTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  /** Hosts Bob's OH (the message destination). */
  private ServerContext bobHost;

  /** Return-path relay. */
  private ServerContext relay;

  /** Hosts Alice's ack OH (the R-ACK destination). */
  private ServerContext aliceHost;

  private InboundCommandProcessor bobProcessor;
  private InboundCommandProcessor relayProcessor;
  private InboundCommandProcessor aliceProcessor;
  private OutboundMailboxStore bobMailbox;
  private OutboundMailboxStore aliceMailbox;
  private OutboundHandleStore bobHandles;

  private byte[] bobOhId;
  private byte[] aliceOhId;
  private byte[] ackSessionTag;

  @Before
  public void setUp() {
    bobHost = ServerContext.buildDefaultServerContext();
    bobHandles = new OutboundHandleStore();
    bobMailbox = new OutboundMailboxStore();
    bobHost.setOutboundService(new OutboundService(bobHandles, bobMailbox));
    bobProcessor = new InboundCommandProcessor(bobHost);

    relay = ServerContext.buildDefaultServerContext();
    relay.setOutboundService(
        new OutboundService(new OutboundHandleStore(), new OutboundMailboxStore()));
    relayProcessor = new InboundCommandProcessor(relay);

    aliceHost = ServerContext.buildDefaultServerContext();
    OutboundHandleStore aliceHandles = new OutboundHandleStore();
    aliceMailbox = new OutboundMailboxStore();
    aliceHost.setOutboundService(new OutboundService(aliceHandles, aliceMailbox));
    aliceProcessor = new InboundCommandProcessor(aliceHost);

    bobOhId = randomBytes(KademliaId.ID_LENGTH_BYTES);
    aliceOhId = randomBytes(KademliaId.ID_LENGTH_BYTES);
    ackSessionTag = randomBytes(FlaschenpostV2.SESSION_TAG_LEN);

    long now = System.currentTimeMillis();
    bobHandles.put(bobOhId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));
    aliceHandles.put(
        aliceOhId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));
  }

  private static byte[] randomBytes(int len) {
    byte[] bytes = new byte[len];
    RANDOM.nextBytes(bytes);
    return bytes;
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

  /** A connected sender peer (the packet origin, e.g. the sending light client). */
  private static Peer sender(ServerContext host, int port) {
    Peer peer = new Peer("127.0.0.1", port, host.getNodeId());
    peer.setConnected(true);
    peer.writeBuffer = ByteBuffer.allocate(65536);
    host.getPeerList().add(peer);
    return peer;
  }

  /** Reads one frame with the expected command byte from the peer's write buffer. */
  private static byte[] readFrame(Peer peer, byte expectedCommand) {
    ByteBuffer out = peer.writeBuffer;
    out.flip();
    assertThat(out.hasRemaining()).as("expected a frame on the wire").isTrue();
    assertThat(out.get()).isEqualTo(expectedCommand);
    byte[] payload = new byte[out.getInt()];
    out.get(payload);
    assertThat(out.hasRemaining()).as("exactly one frame expected").isFalse();
    out.clear();
    return payload;
  }

  /** Builds a return path pointing at Alice's ack OH via the given hops. */
  private ReturnPath returnPath(ServerContext... hops) {
    ReturnPath.Hop[] descriptors = new ReturnPath.Hop[hops.length];
    for (int i = 0; i < hops.length; i++) {
      descriptors[i] =
          new ReturnPath.Hop(
              hops[i].getNonce(), hops[i].getNodeId().getEncryptionPubKey().getEncoded());
    }
    return new ReturnPath(aliceOhId, ackSessionTag, List.of(descriptors));
  }

  /** Builds the CMD_DELIVER_ACKED plaintext. {@code sessionTag} may be empty (untagged). */
  private byte[] buildAckedDeliverPlaintext(
      byte[] ohId, byte[] sessionTag, byte[] returnPathBytes, byte[] payload) {
    ByteBuffer deliver =
        ByteBuffer.allocate(
            1
                + KademliaId.ID_LENGTH_BYTES
                + 1
                + sessionTag.length
                + returnPathBytes.length
                + 4
                + payload.length);
    deliver.put(FlaschenpostV2.CMD_DELIVER_ACKED);
    deliver.put(ohId);
    deliver.put((byte) sessionTag.length);
    deliver.put(sessionTag);
    deliver.put(returnPathBytes);
    deliver.putInt(payload.length);
    deliver.put(payload);
    return deliver.array();
  }

  /** Encrypts a single-layer packet carrying {@code plaintext} for {@code station}. */
  private static byte[] singleLayerPacket(ServerContext station, byte[] plaintext)
      throws Exception {
    byte[] body =
        FlaschenpostV2.encryptLayer(
            station.getNodeId().getEncryptionPubKey(), station.getNonce(), plaintext);
    return FlaschenpostV2.buildPacket(RANDOM.nextInt(), station.getNonce(), body);
  }

  private RoutingAck fetchSingleRAck() throws Exception {
    List<MailItem> items = aliceMailbox.fetchMessages(aliceOhId, 10, 0);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).getSessionTag().toByteArray())
        .as("the R-ACK must be tagged with the ack session tag")
        .isEqualTo(ackSessionTag);
    return RoutingAck.parseFrom(items.get(0).getPayload().toByteArray());
  }

  @Test
  public void ackedDeliver_depositsMessage_andRAckTraversesReturnPathHops() throws Exception {
    Peer bobToRelay = connect(bobHost, relay, 9601);
    Peer relayToAlice = connect(relay, aliceHost, 9602);

    byte[] payload = "acked forward message".getBytes(StandardCharsets.UTF_8);
    byte[] plaintext =
        buildAckedDeliverPlaintext(
            bobOhId, new byte[0], returnPath(relay, aliceHost).serialize(), payload);
    bobProcessor.parseCommand(
        Command.FLASCHENPOST_V2,
        buildFrame(singleLayerPacket(bobHost, plaintext)),
        sender(bobHost, 9600));

    // the message is deposited exactly like an untagged deliver
    List<MailItem> bobItems = bobMailbox.fetchMessages(bobOhId, 10, 0);
    assertThat(bobItems).hasSize(1);
    assertThat(bobItems.get(0).getPayload().toByteArray()).isEqualTo(payload);
    assertThat(bobItems.get(0).getSessionTag().isEmpty()).isTrue();

    // the R-ACK onion leaves toward the first return hop and traverses like a normal MS04 packet
    byte[] toRelay = readFrame(bobToRelay, Command.FLASCHENPOST_V2);
    assertThat(toRelay).hasSize(FlaschenpostV2.PACKET_SIZE);
    relayProcessor.parseCommand(Command.FLASCHENPOST_V2, buildFrame(toRelay), sender(relay, 9603));
    byte[] toAlice = readFrame(relayToAlice, Command.FLASCHENPOST_V2);
    aliceProcessor.parseCommand(
        Command.FLASCHENPOST_V2, buildFrame(toAlice), sender(aliceHost, 9604));

    RoutingAck rAck = fetchSingleRAck();
    assertThat(rAck.getStatus()).isEqualTo(RoutingAckSender.STATUS_STORED);
    assertThat(rAck.getTimestampMs()).isGreaterThan(0);
  }

  @Test
  public void ackedDeliver_withSessionTag_storesTag_andZeroHopRAckIsDepositedLocally()
      throws Exception {
    // Alice's ack OH lives on the same node that hosts Bob's OH: hop_count = 0 means the
    // depositing node delivers the R-ACK itself, without building an onion
    long now = System.currentTimeMillis();
    bobHandles.put(
        aliceOhId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));

    byte[] messageTag = randomBytes(FlaschenpostV2.SESSION_TAG_LEN);
    byte[] payload = "acked tagged reply".getBytes(StandardCharsets.UTF_8);
    byte[] plaintext =
        buildAckedDeliverPlaintext(bobOhId, messageTag, returnPath().serialize(), payload);
    bobProcessor.parseCommand(
        Command.FLASCHENPOST_V2,
        buildFrame(singleLayerPacket(bobHost, plaintext)),
        sender(bobHost, 9610));

    List<MailItem> bobItems = bobMailbox.fetchMessages(bobOhId, 10, 0);
    assertThat(bobItems).hasSize(1);
    assertThat(bobItems.get(0).getSessionTag().toByteArray())
        .as("the message session tag must be stored like a tagged deliver")
        .isEqualTo(messageTag);

    List<MailItem> ackItems = bobMailbox.fetchMessages(aliceOhId, 10, 0);
    assertThat(ackItems).hasSize(1);
    assertThat(ackItems.get(0).getSessionTag().toByteArray()).isEqualTo(ackSessionTag);
    RoutingAck rAck = RoutingAck.parseFrom(ackItems.get(0).getPayload().toByteArray());
    assertThat(rAck.getStatus()).isEqualTo(RoutingAckSender.STATUS_STORED);
  }

  @Test
  public void ackedDeliver_mailboxFull_sendsMailboxFullRAck() throws Exception {
    long now = System.currentTimeMillis();
    bobHandles.put(
        aliceOhId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));
    // fill Bob's mailbox to the item cap so the acked deliver is rejected with QUOTA_EXCEEDED
    OutboundService bobService = bobHost.getOutboundService();
    byte[] filler = new byte[16];
    while (bobService.depositMessage(bobOhId, filler) == OutboundService.DepositResult.DEPOSITED) {
      // fill until the cap rejects
    }

    byte[] plaintext =
        buildAckedDeliverPlaintext(
            bobOhId, new byte[0], returnPath().serialize(), "over quota".getBytes());
    bobProcessor.parseCommand(
        Command.FLASCHENPOST_V2,
        buildFrame(singleLayerPacket(bobHost, plaintext)),
        sender(bobHost, 9620));

    List<MailItem> ackItems = bobMailbox.fetchMessages(aliceOhId, 10, 0);
    assertThat(ackItems).hasSize(1);
    RoutingAck rAck = RoutingAck.parseFrom(ackItems.get(0).getPayload().toByteArray());
    assertThat(rAck.getStatus()).isEqualTo(RoutingAckSender.STATUS_MAILBOX_FULL);
  }

  @Test
  public void ackedDeliver_invalidTagLength_isDroppedSilently() throws Exception {
    // tag_len must be 0 or 16 — a layer claiming 5 is malformed and must not deposit anything
    byte[] returnPathBytes = returnPath().serialize();
    byte[] tag5 = randomBytes(5);
    ByteBuffer deliver =
        ByteBuffer.allocate(
            1 + KademliaId.ID_LENGTH_BYTES + 1 + tag5.length + returnPathBytes.length + 4);
    deliver.put(FlaschenpostV2.CMD_DELIVER_ACKED);
    deliver.put(bobOhId);
    deliver.put((byte) tag5.length);
    deliver.put(tag5);
    deliver.put(returnPathBytes);
    deliver.putInt(0);
    bobProcessor.parseCommand(
        Command.FLASCHENPOST_V2,
        buildFrame(singleLayerPacket(bobHost, deliver.array())),
        sender(bobHost, 9630));

    assertThat(bobMailbox.fetchMessages(bobOhId, 10, 0)).isEmpty();
  }

  @Test
  public void ackedDeliver_hopCountAboveLimit_isDroppedSilently() throws Exception {
    // a return path claiming more than ReturnPath.MAX_HOPS hops is structurally invalid
    ByteBuffer rp = ByteBuffer.allocate(ReturnPath.FIXED_LEN + 5 * ReturnPath.HOP_LEN);
    rp.put(aliceOhId);
    rp.put(ackSessionTag);
    rp.put((byte) 5);
    rp.put(randomBytes(5 * ReturnPath.HOP_LEN));
    byte[] plaintext =
        buildAckedDeliverPlaintext(bobOhId, new byte[0], rp.array(), "payload".getBytes());
    bobProcessor.parseCommand(
        Command.FLASCHENPOST_V2,
        buildFrame(singleLayerPacket(bobHost, plaintext)),
        sender(bobHost, 9640));

    assertThat(bobMailbox.fetchMessages(bobOhId, 10, 0)).isEmpty();
  }

  @Test
  public void ackedDeliver_truncatedReturnPath_isDroppedSilently() throws Exception {
    // hop_count = 2 but only one hop descriptor present: the payload_len read then overlaps the
    // descriptor bytes — parse must fail on the length checks, nothing may be deposited
    ByteBuffer rp = ByteBuffer.allocate(ReturnPath.FIXED_LEN + ReturnPath.HOP_LEN);
    rp.put(aliceOhId);
    rp.put(ackSessionTag);
    rp.put((byte) 2);
    rp.put(randomBytes(ReturnPath.HOP_LEN));
    ByteBuffer deliver = ByteBuffer.allocate(1 + KademliaId.ID_LENGTH_BYTES + 1 + rp.capacity());
    deliver.put(FlaschenpostV2.CMD_DELIVER_ACKED);
    deliver.put(bobOhId);
    deliver.put((byte) 0);
    deliver.put(rp.array());
    bobProcessor.parseCommand(
        Command.FLASCHENPOST_V2,
        buildFrame(singleLayerPacket(bobHost, deliver.array())),
        sender(bobHost, 9650));

    assertThat(bobMailbox.fetchMessages(bobOhId, 10, 0)).isEmpty();
  }

  @Test
  public void ackedDeliverForRemoteOh_conservesReturnPathOnMs02bForward_andHostAcks()
      throws Exception {
    // the final garlic hop (relay) does not host Bob's OH: it must forward a FlaschenpostPut
    // carrying oh_id, payload AND return_path toward the host node, which then acks
    relay
        .getKadStoreManager()
        .put(OhDht.buildAnnounceContent(bobOhId, bobHost.getNonce(), System.currentTimeMillis()));
    Peer relayToBob = connect(relay, bobHost, 9661);
    Peer bobToAlice = connect(bobHost, aliceHost, 9662);

    byte[] payload = "acked deliver for remote oh".getBytes(StandardCharsets.UTF_8);
    byte[] returnPathBytes = returnPath(aliceHost).serialize();
    byte[] plaintext = buildAckedDeliverPlaintext(bobOhId, new byte[0], returnPathBytes, payload);
    relayProcessor.parseCommand(
        Command.FLASCHENPOST_V2,
        buildFrame(singleLayerPacket(relay, plaintext)),
        sender(relay, 9660));

    byte[] putBytes = readFrame(relayToBob, Command.FLASCHENPOST_PUT);
    im.redpanda.proto.FlaschenpostPut put = im.redpanda.proto.FlaschenpostPut.parseFrom(putBytes);
    assertThat(put.getOhId().toByteArray()).isEqualTo(bobOhId);
    assertThat(put.getContent().toByteArray()).isEqualTo(payload);
    assertThat(put.getReturnPath().toByteArray())
        .as("the return path must survive the MS02b fallback")
        .isEqualTo(returnPathBytes);

    // the host node deposits and sends the R-ACK onion toward Alice's host (single return hop)
    bobProcessor.parseCommand(
        Command.FLASCHENPOST_PUT, buildFrame(putBytes), sender(bobHost, 9663));
    assertThat(bobMailbox.fetchMessages(bobOhId, 10, 0)).hasSize(1);
    byte[] toAlice = readFrame(bobToAlice, Command.FLASCHENPOST_V2);
    aliceProcessor.parseCommand(
        Command.FLASCHENPOST_V2, buildFrame(toAlice), sender(aliceHost, 9664));

    RoutingAck rAck = fetchSingleRAck();
    assertThat(rAck.getStatus()).isEqualTo(RoutingAckSender.STATUS_STORED);
  }

  @Test
  public void flaschenpostPut_atHopLimitForUnknownOh_sendsHandleExpiredRAck() throws Exception {
    // a forwarded acked deposit arrives at the hop limit on a node that does not host the OH:
    // forwarding is no longer possible, the sender gets a handle_expired R-ACK
    Peer relayToAlice = connect(relay, aliceHost, 9671);

    im.redpanda.proto.FlaschenpostPut put =
        im.redpanda.proto.FlaschenpostPut.newBuilder()
            .setContent(com.google.protobuf.ByteString.copyFrom("late packet".getBytes()))
            .setOhId(com.google.protobuf.ByteString.copyFrom(bobOhId))
            .setHopCount(OhForwarder.MAX_HOPS)
            .setReturnPath(
                com.google.protobuf.ByteString.copyFrom(returnPath(aliceHost).serialize()))
            .build();
    relayProcessor.parseCommand(
        Command.FLASCHENPOST_PUT, buildFrame(put.toByteArray()), sender(relay, 9670));

    byte[] toAlice = readFrame(relayToAlice, Command.FLASCHENPOST_V2);
    aliceProcessor.parseCommand(
        Command.FLASCHENPOST_V2, buildFrame(toAlice), sender(aliceHost, 9672));

    RoutingAck rAck = fetchSingleRAck();
    assertThat(rAck.getStatus()).isEqualTo(RoutingAckSender.STATUS_HANDLE_EXPIRED);
  }

  @Test
  public void flaschenpostPut_withMalformedReturnPath_rejectsDeposit() throws Exception {
    im.redpanda.proto.FlaschenpostPut put =
        im.redpanda.proto.FlaschenpostPut.newBuilder()
            .setContent(com.google.protobuf.ByteString.copyFrom("payload".getBytes()))
            .setOhId(com.google.protobuf.ByteString.copyFrom(bobOhId))
            .setReturnPath(com.google.protobuf.ByteString.copyFrom(randomBytes(7)))
            .build();
    bobProcessor.parseCommand(
        Command.FLASCHENPOST_PUT, buildFrame(put.toByteArray()), sender(bobHost, 9680));

    assertThat(bobMailbox.fetchMessages(bobOhId, 10, 0))
        .as("a structurally invalid return path must reject the whole deposit")
        .isEmpty();
  }

  @Test
  public void returnPath_serializeParse_roundtrips() {
    ReturnPath original = returnPath(relay, aliceHost);
    byte[] serialized = original.serialize();
    assertThat(serialized).hasSize(ReturnPath.FIXED_LEN + 2 * ReturnPath.HOP_LEN);

    ReturnPath parsed = ReturnPath.parseExact(serialized);
    assertThat(parsed).isNotNull();
    assertThat(parsed.ackOhId()).isEqualTo(aliceOhId);
    assertThat(parsed.ackSessionTag()).isEqualTo(ackSessionTag);
    assertThat(parsed.hops()).hasSize(2);
    assertThat(parsed.hops().get(0).kademliaId()).isEqualTo(relay.getNonce());
    assertThat(parsed.hops().get(1).encryptionPub())
        .isEqualTo(aliceHost.getNodeId().getEncryptionPubKey().getEncoded());
  }

  @Test
  public void returnPath_constructor_rejectsInvalidComponents() {
    ReturnPath.Hop hop =
        new ReturnPath.Hop(relay.getNonce(), relay.getNodeId().getEncryptionPubKey().getEncoded());
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> new ReturnPath(randomBytes(19), ackSessionTag, List.of(hop)))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> new ReturnPath(aliceOhId, randomBytes(15), List.of(hop)))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> new ReturnPath(aliceOhId, ackSessionTag, List.of(hop, hop, hop, hop, hop)))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                new ReturnPath(
                    aliceOhId,
                    ackSessionTag,
                    List.of(new ReturnPath.Hop(relay.getNonce(), randomBytes(31)))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void returnPath_parseExact_rejectsTrailingBytes() {
    byte[] serialized = returnPath(relay).serialize();
    byte[] withTrailing = new byte[serialized.length + 1];
    System.arraycopy(serialized, 0, withTrailing, 0, serialized.length);
    assertThat(ReturnPath.parseExact(withTrailing)).isNull();
  }
}
