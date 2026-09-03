package im.redpanda.core;

import static com.google.protobuf.ByteString.copyFrom;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import im.redpanda.mailbox.OhId;
import im.redpanda.mailbox.OutboundHandleStore;
import im.redpanda.mailbox.OutboundHandleStore.HandleRecord;
import im.redpanda.mailbox.OutboundMailboxStore;
import im.redpanda.mailbox.OutboundService;
import im.redpanda.mailbox.OutboundStore;
import im.redpanda.outbound.v1.MailItem;
import im.redpanda.proto.FlaschenpostPut;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@code oh_id} routing path introduced in MS01 for {@code FLASCHENPOST_PUT} handling
 * in {@link InboundCommandProcessor}.
 *
 * <p>Covers: direct-OH deposit when {@code oh_id} is present and registered, the authoritative
 * explicit-oh_id path introduced in MS02b (rejected/unknown deposits are dropped, opt-in status
 * responses for light clients), and backward compatibility when {@code oh_id} is absent.
 */
class InboundCommandProcessorFlaschenpostPutTest {

  private static final int HANDLE_KEY_LENGTH = 32;
  private static final long HANDLE_EXPIRY_MILLIS = 60_000;

  private ServerContext ctx;
  private InboundCommandProcessor proc;
  private OutboundHandleStore handleStore;
  private OutboundMailboxStore mailboxStore;

  @BeforeEach
  void setup() {
    ctx = ServerContext.buildDefaultServerContext();
    OutboundStore outboundStore = OutboundStore.inMemory();
    handleStore = outboundStore.handles();
    mailboxStore = outboundStore.mailbox();
    OutboundService outboundService = new OutboundService(outboundStore);
    ctx.setOutboundService(outboundService);
    proc = new InboundCommandProcessor(ctx);
  }

  /** Builds a [4-byte len][payload] frame as expected by {@code parseCommand}. */
  private static ByteBuffer buildFrame(byte[] payload) {
    ByteBuffer buf = ByteBuffer.allocate(4 + payload.length);
    buf.putInt(payload.length);
    buf.put(payload);
    buf.flip();
    return buf;
  }

  /** Returns a 20-byte oh_id suitable for registration and deposit tests. */
  private static OhId sampleOhId() {
    byte[] id = new byte[KademliaId.ID_LENGTH_BYTES];
    for (int i = 0; i < id.length; i++) {
      id[i] = (byte) (i + 1);
    }
    return OhId.fromBytes(id);
  }

  /** Registers an OH handle with a future expiry in the in-memory handle store. */
  private void registerOh(OhId ohId) {
    long now = System.currentTimeMillis();
    HandleRecord record =
        new HandleRecord(new byte[HANDLE_KEY_LENGTH], now, now + HANDLE_EXPIRY_MILLIS);
    handleStore.put(ohId, record);
  }

  /**
   * When {@code oh_id} is present and matches a registered OH, the message is deposited directly
   * into the mailbox and the handler returns early (GMParser is not invoked).
   */
  @Test
  void flaschenpostPut_withValidRegisteredOhId_depositsToMailbox() {
    OhId ohId = sampleOhId();
    registerOh(ohId);

    byte[] content = "direct-oh-payload".getBytes(StandardCharsets.UTF_8);
    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(content))
            .setOhId(ohId.toByteString())
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9001, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);

    // Verify the message was deposited into the mailbox (confirms early return via oh_id path)
    List<MailItem> items = mailboxStore.fetchMessages(ohId, 10, 0);
    assertEquals(1, items.size());
    assertArrayEquals(content, items.get(0).getPayload().toByteArray());
  }

  /**
   * MS02b: when {@code oh_id} is present but no matching OH is registered, the packet is dropped on
   * the explicit path — it must NOT fall through to the legacy GMParser, which would misinterpret
   * raw client payloads (e.g. encrypted chat payloads) as GarlicMessages.
   */
  @Test
  void flaschenpostPut_withOhIdButNoRegisteredOh_isDroppedWithoutLegacyFallthrough() {
    OhId ohId = sampleOhId();
    // Intentionally skip registerOh() so depositMessage returns NOT_FOUND

    // A raw (non-garlic) payload like the mobile client sends; legacy parsing would throw on it
    byte[] rawPayload = new byte[64];
    rawPayload[0] = 0x02; // not a known GMType

    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(rawPayload))
            .setOhId(ohId.toByteString())
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9002, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);

    // Mailbox must be empty – deposit was not performed
    List<MailItem> items = mailboxStore.fetchMessages(ohId, 10, 0);
    assertEquals(0, items.size());
  }

  /**
   * MS02b: when {@code oh_id} has an invalid byte length, the packet is malformed and dropped (a
   * light client with want_response would receive BAD_REQUEST).
   */
  @Test
  void flaschenpostPut_withInvalidOhIdLength_isDropped() {
    byte[] wrongLengthId = new byte[5]; // too short, not ID_LENGTH_BYTES

    byte[] rawPayload = new byte[64];
    rawPayload[0] = 0x02;

    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(rawPayload))
            .setOhId(ByteString.copyFrom(wrongLengthId))
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9003, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);
  }

  /**
   * When {@code oh_id} is absent (pre-MS01 message), the handler uses the legacy GMParser path.
   * This ensures backward compatibility with messages that do not carry an explicit {@code oh_id}.
   */
  @Test
  void flaschenpostPut_withoutOhId_usesLegacyPathWithoutError() {
    // Build GMAck payload with no oh_id field – mimics pre-MS01 behavior
    ByteBuffer ack = ByteBuffer.allocate(1 + 4 + 4);
    ack.put(im.redpanda.flaschenpost.GMType.ACK.getId());
    ack.putInt(4);
    ack.putInt(55);
    ack.flip();
    byte[] ackBytes = new byte[ack.remaining()];
    ack.get(ackBytes);

    FlaschenpostPut putMsg = FlaschenpostPut.newBuilder().setContent(copyFrom(ackBytes)).build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9004, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);
  }

  /**
   * When {@code outboundService} is null (not configured) and {@code oh_id} is present, the handler
   * skips direct deposit and falls through to the legacy GMParser path.
   */
  @Test
  void flaschenpostPut_withOhIdButNullOutboundService_fallsThroughToLegacy() {
    // Build a ServerContext without OutboundService
    ServerContext noServiceCtx = ServerContext.buildDefaultServerContext();
    InboundCommandProcessor noServiceProc = new InboundCommandProcessor(noServiceCtx);

    OhId ohId = sampleOhId();
    byte[] ackBytes = buildAckPayload(42);

    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(ackBytes))
            .setOhId(ohId.toByteString())
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9005, noServiceCtx.getNodeId());
    peer.setConnected(true);
    noServiceCtx.getPeerList().add(peer);

    int consumed = noServiceProc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);
  }

  /**
   * REDPANDAJ-2DR (Copilot review finding): the empty-oh_id BAD_REQUEST pre-check must be scoped to
   * {@code oh_id.isEmpty()}, not merely "the MS01 direct-deposit block was skipped" — a non-empty
   * oh_id also falls through to the legacy path when {@code outboundService} is null (server
   * misconfiguration), and that pre-existing fallthrough behavior must be preserved unconditionally
   * rather than being newly rejected as if oh_id were absent.
   */
  @Test
  void flaschenpostPut_withOhIdButNullOutboundService_invalidContentIsNotRejected() {
    ServerContext noServiceCtx = ServerContext.buildDefaultServerContext();
    InboundCommandProcessor noServiceProc = new InboundCommandProcessor(noServiceCtx);

    OhId ohId = sampleOhId();
    // Content that is not a valid GM frame at all (unknown type byte) — would trip the new
    // isValidFrame pre-check if it were wrongly applied here.
    byte[] invalidContent = new byte[] {(byte) 0x2a, 1, 2, 3};

    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(invalidContent))
            .setOhId(ohId.toByteString())
            .setWantResponse(true)
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9014, noServiceCtx.getNodeId());
    peer.setConnected(true);
    peer.setLightClient(true);
    peer.writeBuffer = ByteBuffer.allocate(8192);
    noServiceCtx.getPeerList().add(peer);

    int consumed = noServiceProc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);
    // respondToDeposit requires a non-null outboundService, so no response is written either way
    // — but the point of this test is that reaching that point never throws or misclassifies a
    // non-empty oh_id as the empty-oh_id legacy case.
    assertEquals(0, peer.writeBuffer.position());
  }

  /**
   * When {@code oh_id} is absent and the content is a valid GarlicMessage with a destination that
   * matches a registered OH, {@code tryDepositToLocalOh} deposits the message and returns early.
   */
  @Test
  void flaschenpostPut_legacyPathDepositsViaGarlicMessageDestination() {
    OhId ohId = sampleOhId();
    registerOh(ohId);

    // Build a GarlicMessage-formatted payload: [1 gmType][4 overallLen][20 destinationId][data]
    byte[] extraData = "legacy-payload-body".getBytes(StandardCharsets.UTF_8);
    int overallLen = 4 + KademliaId.ID_LENGTH_BYTES + extraData.length;
    ByteBuffer gm = ByteBuffer.allocate(1 + 4 + KademliaId.ID_LENGTH_BYTES + extraData.length);
    gm.put(im.redpanda.flaschenpost.GMType.GARLIC_MESSAGE.getId());
    gm.putInt(overallLen);
    ohId.writeTo(gm);
    gm.put(extraData);
    gm.flip();
    byte[] gmBytes = new byte[gm.remaining()];
    gm.get(gmBytes);

    // No oh_id set → handler will try tryDepositToLocalOh → deposit succeeds
    FlaschenpostPut putMsg = FlaschenpostPut.newBuilder().setContent(copyFrom(gmBytes)).build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9006, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);

    // Verify the message was deposited via the legacy tryDepositToLocalOh path
    List<MailItem> items = mailboxStore.fetchMessages(ohId, 10, 0);
    assertEquals(1, items.size());
    assertArrayEquals(gmBytes, items.get(0).getPayload().toByteArray());
  }

  /**
   * When {@code oh_id} is absent and content is too short for a GarlicMessage header, {@code
   * tryDepositToLocalOh} returns false. The handler then falls through to GMParser which processes
   * the valid ACK payload. This exercises the {@code content.length < headerLen} guard.
   */
  @Test
  void flaschenpostPut_withContentShorterThanGarlicHeader_tryDepositReturnsFalse() {
    // ACK payload is 9 bytes, shorter than GarlicMessage header (25 bytes)
    byte[] ackBytes = buildAckPayload(33);

    FlaschenpostPut putMsg = FlaschenpostPut.newBuilder().setContent(copyFrom(ackBytes)).build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9007, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);
  }

  /**
   * When {@code oh_id} targets an expired OH handle, deposit fails (NOT_FOUND) and the packet is
   * dropped on the explicit path.
   */
  @Test
  void flaschenpostPut_withExpiredOhHandle_isDropped() {
    OhId ohId = sampleOhId();
    // Register with an already-expired handle
    long now = System.currentTimeMillis();
    HandleRecord expiredRecord = new HandleRecord(new byte[HANDLE_KEY_LENGTH], now - 2000, now - 1);
    handleStore.put(ohId, expiredRecord);

    byte[] ackBytes = buildAckPayload(77);
    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(ackBytes))
            .setOhId(ohId.toByteString())
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9008, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);

    // Mailbox must be empty — expired handle should not accept deposit
    List<MailItem> items = mailboxStore.fetchMessages(ohId, 10, 0);
    assertEquals(0, items.size());
  }

  /**
   * Multiple messages deposited to the same OH mailbox are all retrievable and maintain their
   * ordering.
   */
  @Test
  void flaschenpostPut_multipleDepositsToSameOh_allStored() {
    OhId ohId = sampleOhId();
    registerOh(ohId);

    Peer peer = new Peer("127.0.0.1", 9009, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    byte[][] payloads = {
      "msg-one".getBytes(StandardCharsets.UTF_8),
      "msg-two".getBytes(StandardCharsets.UTF_8),
      "msg-three".getBytes(StandardCharsets.UTF_8)
    };

    for (byte[] payload : payloads) {
      FlaschenpostPut putMsg =
          FlaschenpostPut.newBuilder()
              .setContent(copyFrom(payload))
              .setOhId(ohId.toByteString())
              .build();
      byte[] putData = putMsg.toByteArray();
      proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);
    }

    List<MailItem> items = mailboxStore.fetchMessages(ohId, 10, 0);
    assertEquals(3, items.size());
    assertArrayEquals(payloads[0], items.get(0).getPayload().toByteArray());
    assertArrayEquals(payloads[1], items.get(1).getPayload().toByteArray());
    assertArrayEquals(payloads[2], items.get(2).getPayload().toByteArray());
  }

  /**
   * When {@code oh_id} is empty bytes (zero-length), the handler treats it the same as absent and
   * falls through to the legacy path.
   */
  @Test
  void flaschenpostPut_withEmptyOhId_usesLegacyPath() {
    byte[] ackBytes = buildAckPayload(88);
    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(ackBytes))
            .setOhId(ByteString.EMPTY)
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9010, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);
  }

  /**
   * REDPANDAJ-2DR regression: a light client sent a FlaschenpostPut with empty oh_id and content =
   * an end-to-end encrypted payload whose first byte is 0x04 (== GMType.ACK). The legacy path fell
   * into GMParser, GMAck.parseContent threw on the bad length, and the RuntimeException unwound the
   * reader loop, leaving the connection retrying and generating a fresh Sentry event per retry. The
   * handler must now reject the packet (BAD_REQUEST, opt-in response only) without throwing, and
   * consume the frame regardless.
   */
  @Test
  void flaschenpostPut_emptyOhIdWithEncryptedAckLikePayload_isDroppedWithoutThrowing() {
    byte[] encryptedPayload = new byte[48];
    encryptedPayload[0] = im.redpanda.flaschenpost.GMType.ACK.getId(); // 0x04
    for (int i = 1; i < encryptedPayload.length; i++) {
      encryptedPayload[i] = (byte) (0x40 + i);
    }

    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(encryptedPayload))
            .setOhId(ByteString.EMPTY)
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9011, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    // Must not throw and must consume the full frame.
    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);
    assertEquals(1 + 4 + putData.length, consumed);
  }

  /**
   * REDPANDAJ-2DR: same malformed/E2E-encrypted-looking payload as above, but from a light client
   * that opted into {@code want_response} — it must receive an explicit BAD_REQUEST instead of
   * silence, so it learns the deposit failed rather than retrying blindly for hours.
   */
  @Test
  void flaschenpostPut_emptyOhIdWithInvalidContent_lightClientWantResponse_getsBadRequest() {
    byte[] encryptedPayload = new byte[48];
    encryptedPayload[0] = im.redpanda.flaschenpost.GMType.ACK.getId(); // 0x04
    for (int i = 1; i < encryptedPayload.length; i++) {
      encryptedPayload[i] = (byte) (0x40 + i);
    }

    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(encryptedPayload))
            .setOhId(ByteString.EMPTY)
            .setWantResponse(true)
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9012, ctx.getNodeId());
    peer.setConnected(true);
    peer.setLightClient(true);
    peer.writeBuffer = ByteBuffer.allocate(8192);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);
    assertEquals(1 + 4 + putData.length, consumed);

    ByteBuffer buf = peer.writeBuffer;
    buf.flip();
    assertEquals(Command.FLASCHENPOST_PUT_RES, buf.get());
    int len = buf.getInt();
    byte[] payload = new byte[len];
    buf.get(payload);
    try {
      assertEquals(
          im.redpanda.outbound.v1.Status.BAD_REQUEST,
          im.redpanda.outbound.v1.FlaschenpostPutResponse.parseFrom(payload).getStatus());
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * REDPANDAJ-2DR: a valid GMAck sent legacy-style (no oh_id) must still be processed normally by
   * GMParser and must NOT be rejected as BAD_REQUEST — only genuinely malformed/unrecognized
   * content triggers the new pre-check.
   */
  @Test
  void flaschenpostPut_emptyOhIdWithValidAck_isNotRejected() {
    byte[] ackBytes = buildAckPayload(123);
    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(ackBytes))
            .setOhId(ByteString.EMPTY)
            .setWantResponse(true)
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9013, ctx.getNodeId());
    peer.setConnected(true);
    peer.setLightClient(true);
    peer.writeBuffer = ByteBuffer.allocate(8192);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);
    assertEquals(1 + 4 + putData.length, consumed);

    // A valid (if legacy) ACK never reaches respondToDeposit at all — no FlaschenpostPutResponse
    // is written, matching pre-existing legacy fire-and-forget behavior.
    assertEquals(0, peer.writeBuffer.position());
  }

  // --- MS02b: opt-in deposit status responses (want_response) ---

  /** Light client with want_response gets an OK FlaschenpostPutResponse on successful deposit. */
  @Test
  void flaschenpostPut_lightClientWantResponse_receivesOkStatus() {
    OhId ohId = sampleOhId();
    registerOh(ohId);

    im.redpanda.outbound.v1.FlaschenpostPutResponse res =
        depositAndReadResponse(ohId, "payload-ok".getBytes(StandardCharsets.UTF_8), true, true);
    assertEquals(im.redpanda.outbound.v1.Status.OK, res.getStatus());
  }

  /**
   * Light client with want_response gets OK when the OH is unknown here but forwarding toward the
   * host node was accepted (MS02b best-effort forwarding).
   */
  @Test
  void flaschenpostPut_lightClientWantResponse_unknownOhAcceptedForForwarding() {
    OhId ohId = sampleOhId(); // not registered

    im.redpanda.outbound.v1.FlaschenpostPutResponse res =
        depositAndReadResponse(ohId, new byte[32], true, true);
    assertEquals(im.redpanda.outbound.v1.Status.OK, res.getStatus());
  }

  /**
   * Oversized payloads are rejected up front with BAD_REQUEST — even for unknown OHs — instead of
   * being forwarded through the network only to be rejected by the host node.
   */
  @Test
  void flaschenpostPut_oversizedPayloadToUnknownOh_isRejectedNotForwarded() {
    OhId ohId = sampleOhId(); // not registered
    byte[] oversized = new byte[OutboundMailboxStore.MAX_ITEM_BYTES + 1];

    im.redpanda.outbound.v1.FlaschenpostPutResponse res =
        depositAndReadResponse(ohId, oversized, true, true);
    assertEquals(im.redpanda.outbound.v1.Status.BAD_REQUEST, res.getStatus());
  }

  /** At the hop limit an unknown OH is NOT forwarded again — the client sees NOT_FOUND. */
  @Test
  void flaschenpostPut_lightClientWantResponse_unknownOhAtHopLimitIsNotFound() {
    OhId ohId = sampleOhId(); // not registered

    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(new byte[32]))
            .setOhId(ohId.toByteString())
            .setWantResponse(true)
            .setHopCount(im.redpanda.flaschenpost.OhForwarder.MAX_HOPS)
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9101, ctx.getNodeId());
    peer.setConnected(true);
    peer.setLightClient(true);
    peer.writeBuffer = ByteBuffer.allocate(8192);
    ctx.getPeerList().add(peer);
    proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    ByteBuffer buf = peer.writeBuffer;
    buf.flip();
    assertEquals(Command.FLASCHENPOST_PUT_RES, buf.get());
    int len = buf.getInt();
    byte[] payload = new byte[len];
    buf.get(payload);
    try {
      assertEquals(
          im.redpanda.outbound.v1.Status.NOT_FOUND,
          im.redpanda.outbound.v1.FlaschenpostPutResponse.parseFrom(payload).getStatus());
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new AssertionError(e);
    }
  }

  /** Full nodes never receive command 158, even if they set want_response. */
  @Test
  void flaschenpostPut_fullNodeWantResponse_receivesNoResponse() {
    OhId ohId = sampleOhId();
    registerOh(ohId);

    Peer peer = depositWithPeer(ohId, new byte[16], true, false);
    assertEquals(
        0, peer.writeBuffer.position(), "no response bytes must be written for full nodes");
  }

  /** Light clients that do not set want_response keep the fire-and-forget behavior. */
  @Test
  void flaschenpostPut_lightClientWithoutWantResponse_receivesNoResponse() {
    OhId ohId = sampleOhId();
    registerOh(ohId);

    Peer peer = depositWithPeer(ohId, new byte[16], false, true);
    assertEquals(0, peer.writeBuffer.position());
  }

  /** Sends a FlaschenpostPut and returns the peer for write-buffer inspection. */
  private Peer depositWithPeer(
      OhId ohId, byte[] content, boolean wantResponse, boolean lightClient) {
    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(content))
            .setOhId(ohId.toByteString())
            .setWantResponse(wantResponse)
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9100, ctx.getNodeId());
    peer.setConnected(true);
    peer.setLightClient(lightClient);
    peer.writeBuffer = ByteBuffer.allocate(8192);
    ctx.getPeerList().add(peer);

    proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);
    return peer;
  }

  /** Deposits and parses the FlaschenpostPutResponse (command 158) from the peer write buffer. */
  private im.redpanda.outbound.v1.FlaschenpostPutResponse depositAndReadResponse(
      OhId ohId, byte[] content, boolean wantResponse, boolean lightClient) {
    Peer peer = depositWithPeer(ohId, content, wantResponse, lightClient);
    ByteBuffer buf = peer.writeBuffer;
    buf.flip();
    assertEquals(Command.FLASCHENPOST_PUT_RES, buf.get());
    int len = buf.getInt();
    byte[] payload = new byte[len];
    buf.get(payload);
    try {
      return im.redpanda.outbound.v1.FlaschenpostPutResponse.parseFrom(payload);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new AssertionError("response payload must be a FlaschenpostPutResponse", e);
    }
  }

  /** Builds a minimal GMAck payload: [1 type][4 len][4 ackId]. */
  private static byte[] buildAckPayload(int ackId) {
    ByteBuffer ack = ByteBuffer.allocate(1 + 4 + 4);
    ack.put(im.redpanda.flaschenpost.GMType.ACK.getId());
    ack.putInt(4);
    ack.putInt(ackId);
    ack.flip();
    byte[] bytes = new byte[ack.remaining()];
    ack.get(bytes);
    return bytes;
  }
}
