package im.redpanda.flaschenpost;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import im.redpanda.core.Command;
import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.outbound.OhDht;
import im.redpanda.outbound.OutboundHandleStore;
import im.redpanda.outbound.OutboundMailboxStore;
import im.redpanda.outbound.OutboundService;
import im.redpanda.outbound.OutboundStore;
import im.redpanda.outbound.v1.FlaschenpostPutResponse;
import im.redpanda.outbound.v1.MailItem;
import im.redpanda.outbound.v1.RoutingAck;
import im.redpanda.outbound.v1.Status;
import im.redpanda.proto.FlaschenpostPut;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the deposit/forward/R-ACK policy extracted from {@code
 * InboundCommandProcessor.handleFlaschenpostPut} (DDD review 2026-08-31, Top-3 item 3).
 *
 * <p>The policy is exercised directly (no wire frame, no {@code parseCommand}) — the end-to-end
 * behaviour through the parser stays covered by {@code InboundCommandProcessorFlaschenpostPutTest},
 * {@code OhForwarderTest} and {@code RoutingAckRouterTest}.
 */
class MailboxDepositPolicyTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  /** The node running the policy (hosts Bob's OH in the local-deposit tests). */
  private ServerContext node;

  private OutboundService outboundService;
  private OutboundHandleStore handleStore;
  private OutboundMailboxStore mailboxStore;

  /** A second node used as the resolved OH host in the forwarding tests. */
  private ServerContext hostNode;

  private byte[] ohId;

  @BeforeEach
  void setUp() {
    node = ServerContext.buildDefaultServerContext();
    OutboundStore outboundStore = OutboundStore.inMemory();
    handleStore = outboundStore.handles();
    mailboxStore = outboundStore.mailbox();
    outboundService = new OutboundService(outboundStore);
    node.setOutboundService(outboundService);

    hostNode = ServerContext.buildDefaultServerContext();
    hostNode.setOutboundService(new OutboundService(OutboundStore.inMemory()));

    ohId = randomBytes(KademliaId.ID_LENGTH_BYTES);
  }

  private static byte[] randomBytes(int len) {
    byte[] bytes = new byte[len];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  /** Registers an OH handle with a future expiry on the local node. */
  private void registerOh(byte[] id) {
    long now = System.currentTimeMillis();
    handleStore.put(id, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));
  }

  /** A connected light client that opted into the MS02b status response. */
  private Peer lightClient(int port) {
    Peer peer = new Peer("127.0.0.1", port, node.getNodeId());
    peer.setConnected(true);
    peer.setLightClient(true);
    peer.writeBuffer = ByteBuffer.allocate(65536);
    node.getPeerList().add(peer);
    return peer;
  }

  /** Reads the single FLASCHENPOST_PUT_RES status from the peer's write buffer. */
  private static Status responseStatus(Peer peer) throws Exception {
    ByteBuffer out = peer.writeBuffer;
    out.flip();
    assertThat(out.hasRemaining()).as("expected a status response").isTrue();
    assertThat(out.get()).isEqualTo(Command.FLASCHENPOST_PUT_RES);
    byte[] payload = new byte[out.getInt()];
    out.get(payload);
    return FlaschenpostPutResponse.parseFrom(payload).getStatus();
  }

  /** Registers a local ack OH and returns a hop-less return path pointing at it. */
  private ReturnPath localAckPath() {
    byte[] ackOhId = randomBytes(KademliaId.ID_LENGTH_BYTES);
    registerOh(ackOhId);
    return new ReturnPath(ackOhId, randomBytes(OutboundService.SESSION_TAG_BYTES), List.of());
  }

  /** Fetches the single R-ACK deposited into the local ack mailbox. */
  private RoutingAck singleAck(ReturnPath ackPath) throws Exception {
    List<MailItem> items = mailboxStore.fetchMessages(ackPath.ackOhId(), 10, 0);
    assertThat(items).as("exactly one R-ACK expected").hasSize(1);
    return RoutingAck.parseFrom(items.get(0).getPayload());
  }

  private FlaschenpostPut.Builder put(byte[] content) {
    return FlaschenpostPut.newBuilder()
        .setContent(ByteString.copyFrom(content))
        .setWantResponse(true);
  }

  // --- deposit ---------------------------------------------------------------------------------

  @Test
  void deposit_registeredOh_storesMessageAndAnswersOk() throws Exception {
    registerOh(ohId);
    byte[] content = "hello mailbox".getBytes(StandardCharsets.UTF_8);
    Peer sender = lightClient(9401);

    MailboxDepositPolicy.handlePut(
        node, outboundService, put(content).setOhId(ByteString.copyFrom(ohId)).build(), sender);

    List<MailItem> items = mailboxStore.fetchMessages(ohId, 10, 0);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).getPayload().toByteArray()).isEqualTo(content);
    assertThat(responseStatus(sender)).isEqualTo(Status.OK);
  }

  @Test
  void deposit_answersOnlyLightClientsThatAskedForIt() {
    registerOh(ohId);
    Peer peerWithoutFlag = lightClient(9402);
    Peer fullPeer = new Peer("127.0.0.1", 9403, node.getNodeId());
    fullPeer.setConnected(true);
    fullPeer.writeBuffer = ByteBuffer.allocate(4096);
    node.getPeerList().add(fullPeer);

    MailboxDepositPolicy.handlePut(
        node,
        outboundService,
        FlaschenpostPut.newBuilder()
            .setContent(ByteString.copyFrom(new byte[8]))
            .setOhId(ByteString.copyFrom(ohId))
            .build(),
        peerWithoutFlag);
    MailboxDepositPolicy.handlePut(
        node,
        outboundService,
        put(new byte[8]).setOhId(ByteString.copyFrom(ohId)).build(),
        fullPeer);

    assertThat(peerWithoutFlag.writeBuffer.position())
        .as("no want_response ⇒ no command 158")
        .isZero();
    assertThat(fullPeer.writeBuffer.position())
        .as("full peers desync on command 158 and never receive it")
        .isZero();
  }

  // --- forward ---------------------------------------------------------------------------------

  @Test
  void unknownOh_forwardsTowardResolvedHost_preservingOhIdAndBumpingHopCount() throws Exception {
    node.getKadStoreManager()
        .put(
            OhDht.buildAnnounceContent(
                ohId, hostNode.getNodeId().getKademliaId(), System.currentTimeMillis()));
    Peer hostPeer = new Peer("127.0.0.1", 9404, hostNode.getNodeId());
    hostPeer.setConnected(true);
    hostPeer.writeBuffer = ByteBuffer.allocate(65536);
    node.getPeerList().add(hostPeer);

    byte[] content = "forward me".getBytes(StandardCharsets.UTF_8);
    Peer sender = lightClient(9405);

    MailboxDepositPolicy.handlePut(
        node, outboundService, put(content).setOhId(ByteString.copyFrom(ohId)).build(), sender);

    ByteBuffer out = hostPeer.writeBuffer;
    out.flip();
    assertThat(out.hasRemaining()).as("packet must be forwarded to the resolved host").isTrue();
    assertThat(out.get()).isEqualTo(Command.FLASCHENPOST_PUT);
    byte[] forwardedBytes = new byte[out.getInt()];
    out.get(forwardedBytes);
    FlaschenpostPut forwarded = FlaschenpostPut.parseFrom(forwardedBytes);
    assertThat(forwarded.getOhId().toByteArray()).isEqualTo(ohId);
    assertThat(forwarded.getContent().toByteArray()).isEqualTo(content);
    assertThat(forwarded.getHopCount()).isEqualTo(1);

    assertThat(mailboxStore.fetchMessages(ohId, 10, 0)).isEmpty();
    assertThat(responseStatus(sender))
        .as("forwarding accepted ⇒ best-effort OK")
        .isEqualTo(Status.OK);
  }

  @Test
  void unknownOh_atHopLimit_answersNotFound() throws Exception {
    Peer sender = lightClient(9406);

    MailboxDepositPolicy.handlePut(
        node,
        outboundService,
        put(new byte[8])
            .setOhId(ByteString.copyFrom(ohId))
            .setHopCount(OhForwarder.MAX_HOPS)
            .build(),
        sender);

    assertThat(responseStatus(sender)).isEqualTo(Status.NOT_FOUND);
  }

  // --- R-ACK -----------------------------------------------------------------------------------

  @Test
  void deposit_withReturnPath_sendsStoredRoutingAck() throws Exception {
    registerOh(ohId);
    ReturnPath ackPath = localAckPath();
    Peer sender = lightClient(9407);

    MailboxDepositPolicy.handlePut(
        node,
        outboundService,
        put("acked".getBytes(StandardCharsets.UTF_8))
            .setOhId(ByteString.copyFrom(ohId))
            .setReturnPath(ByteString.copyFrom(ackPath.serialize()))
            .build(),
        sender);

    assertThat(singleAck(ackPath).getStatus()).isEqualTo(RoutingAckSender.STATUS_STORED);
    assertThat(responseStatus(sender)).isEqualTo(Status.OK);
  }

  @Test
  void unknownOh_atHopLimit_withReturnPath_sendsHandleExpiredRoutingAck() throws Exception {
    ReturnPath ackPath = localAckPath();
    Peer sender = lightClient(9408);

    MailboxDepositPolicy.handlePut(
        node,
        outboundService,
        put(new byte[8])
            .setOhId(ByteString.copyFrom(ohId))
            .setHopCount(OhForwarder.MAX_HOPS)
            .setReturnPath(ByteString.copyFrom(ackPath.serialize()))
            .build(),
        sender);

    assertThat(singleAck(ackPath).getStatus()).isEqualTo(RoutingAckSender.STATUS_HANDLE_EXPIRED);
    assertThat(responseStatus(sender)).isEqualTo(Status.NOT_FOUND);
  }

  // --- rejections ------------------------------------------------------------------------------

  @Test
  void invalidOhIdLength_answersBadRequestWithoutDepositing() throws Exception {
    Peer sender = lightClient(9409);

    MailboxDepositPolicy.handlePut(
        node,
        outboundService,
        put(new byte[8]).setOhId(ByteString.copyFrom(new byte[5])).build(),
        sender);

    assertThat(responseStatus(sender)).isEqualTo(Status.BAD_REQUEST);
    assertThat(mailboxStore.fetchMessages(new byte[5], 10, 0)).isEmpty();
  }

  @Test
  void oversizedContent_answersBadRequestBeforeAnyDepositOrForward() throws Exception {
    registerOh(ohId);
    Peer sender = lightClient(9410);

    MailboxDepositPolicy.handlePut(
        node,
        outboundService,
        put(new byte[OutboundMailboxStore.MAX_ITEM_BYTES + 1])
            .setOhId(ByteString.copyFrom(ohId))
            .build(),
        sender);

    assertThat(responseStatus(sender)).isEqualTo(Status.BAD_REQUEST);
    assertThat(mailboxStore.fetchMessages(ohId, 10, 0)).isEmpty();
  }

  @Test
  void invalidSessionTagLength_answersBadRequest() throws Exception {
    registerOh(ohId);
    Peer sender = lightClient(9411);

    MailboxDepositPolicy.handlePut(
        node,
        outboundService,
        put(new byte[8])
            .setOhId(ByteString.copyFrom(ohId))
            .setSessionTag(ByteString.copyFrom(new byte[OutboundService.SESSION_TAG_BYTES - 1]))
            .build(),
        sender);

    assertThat(responseStatus(sender)).isEqualTo(Status.BAD_REQUEST);
    assertThat(mailboxStore.fetchMessages(ohId, 10, 0)).isEmpty();
  }

  @Test
  void malformedReturnPath_answersBadRequestWithoutDepositing() throws Exception {
    registerOh(ohId);
    Peer sender = lightClient(9412);

    MailboxDepositPolicy.handlePut(
        node,
        outboundService,
        put(new byte[8])
            .setOhId(ByteString.copyFrom(ohId))
            .setReturnPath(ByteString.copyFrom(new byte[7]))
            .build(),
        sender);

    assertThat(responseStatus(sender)).isEqualTo(Status.BAD_REQUEST);
    assertThat(mailboxStore.fetchMessages(ohId, 10, 0)).isEmpty();
  }

  @Test
  void oversizedReturnPath_answersBadRequest() throws Exception {
    registerOh(ohId);
    Peer sender = lightClient(9413);

    MailboxDepositPolicy.handlePut(
        node,
        outboundService,
        put(new byte[8])
            .setOhId(ByteString.copyFrom(ohId))
            .setReturnPath(ByteString.copyFrom(new byte[ReturnPath.MAX_SERIALIZED_LEN + 1]))
            .build(),
        sender);

    assertThat(responseStatus(sender)).isEqualTo(Status.BAD_REQUEST);
    assertThat(mailboxStore.fetchMessages(ohId, 10, 0)).isEmpty();
  }

  // --- legacy garlic path ----------------------------------------------------------------------

  @Test
  void emptyOhId_depositsViaGarlicMessageDestination() {
    registerOh(ohId);
    byte[] body = "legacy body".getBytes(StandardCharsets.UTF_8);
    ByteBuffer gm = ByteBuffer.allocate(1 + 4 + KademliaId.ID_LENGTH_BYTES + body.length);
    gm.put(GMType.GARLIC_MESSAGE.getId());
    gm.putInt(4 + KademliaId.ID_LENGTH_BYTES + body.length);
    gm.put(ohId);
    gm.put(body);
    byte[] gmBytes = gm.array();

    Peer sender = lightClient(9414);
    MailboxDepositPolicy.handlePut(
        node,
        outboundService,
        FlaschenpostPut.newBuilder().setContent(ByteString.copyFrom(gmBytes)).build(),
        sender);

    List<MailItem> items = mailboxStore.fetchMessages(ohId, 10, 0);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).getPayload().toByteArray()).isEqualTo(gmBytes);
    assertThat(sender.writeBuffer.position()).as("the legacy path stays fire-and-forget").isZero();
  }

  @Test
  void emptyOhId_withNonGarlicContent_answersBadRequest() throws Exception {
    byte[] encryptedLookingPayload = new byte[48];
    encryptedLookingPayload[0] = GMType.ACK.getId();
    Peer sender = lightClient(9415);

    MailboxDepositPolicy.handlePut(
        node, outboundService, put(encryptedLookingPayload).build(), sender);

    assertThat(responseStatus(sender)).isEqualTo(Status.BAD_REQUEST);
  }

  @Test
  void withoutOutboundService_nonEmptyOhId_keepsLegacyFallthroughAndAnswersNothing() {
    Peer sender = lightClient(9416);

    MailboxDepositPolicy.handlePut(
        node,
        null,
        put(new byte[] {(byte) 0x2a, 1, 2, 3}).setOhId(ByteString.copyFrom(ohId)).build(),
        sender);

    assertThat(sender.writeBuffer.position())
        .as("no outbound service ⇒ no response, and no new rejection either")
        .isZero();
  }
}
