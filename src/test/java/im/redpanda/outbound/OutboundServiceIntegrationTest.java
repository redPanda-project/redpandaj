package im.redpanda.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import im.redpanda.core.Command;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.outbound.v1.AckFetchRequest;
import im.redpanda.outbound.v1.AckFetchResponse;
import im.redpanda.outbound.v1.FetchRequest;
import im.redpanda.outbound.v1.FetchResponse;
import im.redpanda.outbound.v1.RegisterOhRequest;
import im.redpanda.outbound.v1.RegisterOhResponse;
import im.redpanda.outbound.v1.RevokeOhRequest;
import im.redpanda.outbound.v1.RevokeOhResponse;
import im.redpanda.outbound.v1.Status;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import org.junit.Before;
import org.junit.Test;

/**
 * MS01 + MS02 End-to-End Integration Test: register → deposit → fetch (sequence-based) → ackFetch →
 * revoke.
 *
 * <p>Validates the full Outbound Handle lifecycle including sequence_id, next_cursor, mailbox
 * overflow, and delete-after-acknowledge via {@link OutboundService#handleAckFetch}.
 */
public class OutboundServiceIntegrationTest {

  private static final String MSG1 = "msg1";
  private static final String MSG2 = "msg2";
  private static final String MSG3 = "msg3";

  private OutboundService service;
  private OutboundHandleStore handleStore;
  private OutboundMailboxStore mailboxStore;
  private Peer peer;
  private NodeId clientNode;

  @Before
  public void setUp() {
    handleStore = new OutboundHandleStore();
    mailboxStore = new OutboundMailboxStore();
    service = new OutboundService(handleStore, mailboxStore);

    peer = new Peer("127.0.0.1", 12345);
    peer.writeBuffer = ByteBuffer.allocate(8192);
    peer.writeBuffer.clear();
    peer.setConnected(true);

    clientNode = NodeId.generateWithSimpleKey();
  }

  /** Full MS01 lifecycle: register OH → deposit message → fetch message → revoke OH. */
  @Test
  public void testFullLifecycle_Register_Deposit_Fetch_Revoke() throws Exception {
    byte[] ohId = clientNode.getKademliaId().getBytes();

    // 1. Register OH
    RegisterOhRequest regReq = createSignedRegisterRequest();
    service.handleRegister(peer, regReq);

    RegisterOhResponse regRes = readRegisterResponse();
    assertThat(regRes.getStatus()).isEqualTo(Status.OK);
    assertThat(regRes.getExpiresAtMs()).isGreaterThan(System.currentTimeMillis());

    // 2. Deposit a message via depositMessage
    byte[] payload = "Hello from sender!".getBytes(StandardCharsets.UTF_8);
    OutboundService.DepositResult deposited = service.depositMessage(ohId, payload);
    assertThat(deposited)
        .as("Message should be deposited into registered OH")
        .isEqualTo(OutboundService.DepositResult.DEPOSITED);

    // 3. Fetch the deposited message
    FetchRequest fetchReq = createSignedFetchRequest(0);
    service.handleFetch(peer, fetchReq);

    FetchResponse fetchRes = readFetchResponse();
    assertThat(fetchRes.getStatus()).isEqualTo(Status.OK);
    assertThat(fetchRes.getItemsCount()).isEqualTo(1);
    assertThat(fetchRes.getItems(0).getPayload().toStringUtf8()).isEqualTo("Hello from sender!");
    assertThat(fetchRes.getItems(0).getReceivedAtMs()).isGreaterThan(0);

    // 4. Revoke the OH
    RevokeOhRequest revokeReq = createSignedRevokeRequest();
    service.handleRevoke(peer, revokeReq);

    RevokeOhResponse revokeRes = readRevokeResponse();
    assertThat(revokeRes.getStatus()).isEqualTo(Status.OK);

    // 5. Verify OH is gone
    assertThat(handleStore.get(ohId)).isNull();
  }

  @Test
  public void testDepositMessage_OhNotRegistered_ReturnsNotFound() {
    byte[] unknownOhId = new byte[20];
    new SecureRandom().nextBytes(unknownOhId);
    OutboundService.DepositResult deposited =
        service.depositMessage(unknownOhId, "test".getBytes(StandardCharsets.UTF_8));
    assertThat(deposited).isEqualTo(OutboundService.DepositResult.NOT_FOUND);
  }

  @Test
  public void testDepositMessage_MultipleMessages_AllFetched() throws Exception {
    // Register
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse(); // consume

    byte[] ohId = clientNode.getKademliaId().getBytes();

    // Deposit multiple messages
    service.depositMessage(ohId, MSG1.getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, MSG2.getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, MSG3.getBytes(StandardCharsets.UTF_8));

    // Fetch all
    service.handleFetch(peer, createSignedFetchRequest(0));
    FetchResponse fetchRes = readFetchResponse();

    assertThat(fetchRes.getStatus()).isEqualTo(Status.OK);
    assertThat(fetchRes.getItemsCount()).isEqualTo(3);
    assertThat(fetchRes.getItems(0).getPayload().toStringUtf8()).isEqualTo(MSG1);
    assertThat(fetchRes.getItems(1).getPayload().toStringUtf8()).isEqualTo(MSG2);
    assertThat(fetchRes.getItems(2).getPayload().toStringUtf8()).isEqualTo(MSG3);
  }

  /** MS05: a deposit with session tag is returned to the client via FetchResponse. */
  @Test
  public void testDepositWithSessionTag_FetchReturnsTag() throws Exception {
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse(); // consume

    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] sessionTag = new byte[OutboundService.SESSION_TAG_BYTES];
    new SecureRandom().nextBytes(sessionTag);

    OutboundService.DepositResult tagged =
        service.depositMessage(ohId, MSG1.getBytes(StandardCharsets.UTF_8), sessionTag);
    assertThat(tagged).isEqualTo(OutboundService.DepositResult.DEPOSITED);
    // untagged deposits keep working side by side, with an empty tag
    service.depositMessage(ohId, MSG2.getBytes(StandardCharsets.UTF_8));

    service.handleFetch(peer, createSignedFetchRequest(0));
    FetchResponse fetchRes = readFetchResponse();

    assertThat(fetchRes.getStatus()).isEqualTo(Status.OK);
    assertThat(fetchRes.getItemsCount()).isEqualTo(2);
    assertThat(fetchRes.getItems(0).getSessionTag().toByteArray()).isEqualTo(sessionTag);
    assertThat(fetchRes.getItems(1).getSessionTag().isEmpty()).isTrue();
  }

  /** MS05: a non-empty session tag must be exactly 16 bytes. */
  @Test
  public void testDepositWithInvalidSessionTagLength_ReturnsBadRequest() throws Exception {
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse(); // consume

    byte[] ohId = clientNode.getKademliaId().getBytes();
    OutboundService.DepositResult result =
        service.depositMessage(ohId, MSG1.getBytes(StandardCharsets.UTF_8), new byte[7]);
    assertThat(result).isEqualTo(OutboundService.DepositResult.BAD_REQUEST);

    service.handleFetch(peer, createSignedFetchRequest(0));
    assertThat(readFetchResponse().getItemsCount()).isZero();
  }

  @Test
  public void testDepositAfterRevoke_ReturnsFalse() throws Exception {
    byte[] ohId = clientNode.getKademliaId().getBytes();

    // Register
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse();

    // Revoke
    service.handleRevoke(peer, createSignedRevokeRequest());
    readRevokeResponse();

    // Try to deposit after revoke
    OutboundService.DepositResult deposited =
        service.depositMessage(ohId, "late message".getBytes(StandardCharsets.UTF_8));
    assertThat(deposited).isEqualTo(OutboundService.DepositResult.NOT_FOUND);
  }

  @Test
  public void testRevoke_alsoDeletesMailboxItems() throws Exception {
    byte[] ohId = clientNode.getKademliaId().getBytes();

    // Register and deposit messages
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse();
    service.depositMessage(ohId, MSG1.getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, MSG2.getBytes(StandardCharsets.UTF_8));

    // Verify messages exist
    assertThat(mailboxStore.fetchMessages(ohId, 10, 0)).hasSize(2);

    // Revoke
    service.handleRevoke(peer, createSignedRevokeRequest());
    readRevokeResponse();

    // Mailbox items should be cleaned up after revoke
    assertThat(mailboxStore.fetchMessages(ohId, 10, 0)).isEmpty();
  }

  // --- B2 AC: MailItem.message_id is set, 16 bytes, unique, and stable across re-fetch ---

  @Test
  public void testDeposit_messageIdsAreSetUniqueAnd16Bytes() throws Exception {
    byte[] ohId = clientNode.getKademliaId().getBytes();
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse();

    service.depositMessage(ohId, MSG1.getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, MSG2.getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, MSG3.getBytes(StandardCharsets.UTF_8));

    service.handleFetch(peer, createSignedFetchRequest(0));
    FetchResponse res = readFetchResponse();
    assertThat(res.getItemsCount()).isEqualTo(3);

    java.util.Set<String> seen = new java.util.HashSet<>();
    for (int i = 0; i < res.getItemsCount(); i++) {
      ByteString messageId = res.getItems(i).getMessageId();
      assertThat(messageId.isEmpty()).as("message_id must not be empty").isFalse();
      assertThat(messageId.size()).as("message_id must be 16 bytes").isEqualTo(16);
      // outbound.proto contract: "16 bytes UUID raw" — RFC-4122 version 4 + IETF variant bits.
      assertThat(messageId.byteAt(6) & 0xF0)
          .as("message_id must be UUID version 4")
          .isEqualTo(0x40);
      assertThat(messageId.byteAt(8) & 0xC0).as("message_id must use IETF variant").isEqualTo(0x80);
      // Hex of the raw bytes is the frontend dedup key — must be pairwise distinct.
      assertThat(seen.add(toHex(messageId)))
          .as("message_id must be pairwise distinct across deposits")
          .isTrue();
    }
  }

  @Test
  public void testDeposit_messageIdIsStableAcrossRefetch() throws Exception {
    byte[] ohId = clientNode.getKademliaId().getBytes();
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse();

    service.depositMessage(ohId, MSG1.getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, MSG2.getBytes(StandardCharsets.UTF_8));

    // First fetch (no ack) — capture the ids.
    service.handleFetch(peer, createSignedFetchRequest(0));
    FetchResponse first = readFetchResponse();
    assertThat(first.getItemsCount()).isEqualTo(2);
    String id1 = toHex(first.getItems(0).getMessageId());
    String id2 = toHex(first.getItems(1).getMessageId());

    // Re-fetch the same un-acked items — ids must be identical (persisted, not regenerated).
    service.handleFetch(peer, createSignedFetchRequest(0));
    FetchResponse second = readFetchResponse();
    assertThat(second.getItemsCount()).isEqualTo(2);
    assertThat(toHex(second.getItems(0).getMessageId())).isEqualTo(id1);
    assertThat(toHex(second.getItems(1).getMessageId())).isEqualTo(id2);
  }

  private static String toHex(ByteString bytes) {
    StringBuilder sb = new StringBuilder(bytes.size() * 2);
    for (byte b : bytes.toByteArray()) {
      sb.append(Character.forDigit((b >> 4) & 0xF, 16));
      sb.append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }

  // --- MS02 AC: FetchResponse.next_cursor is the highest sequence_id ---

  @Test
  public void testFetch_nextCursorIsHighestSequenceId() throws Exception {
    byte[] ohId = clientNode.getKademliaId().getBytes();
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse();

    service.depositMessage(ohId, "a".getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, "b".getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, "c".getBytes(StandardCharsets.UTF_8));

    // Fetch with limit=2, cursor=0 → gets seqIds 1 and 2
    service.handleFetch(peer, createSignedFetchRequest(0, 2));
    FetchResponse res1 = readFetchResponse();

    assertThat(res1.getStatus()).isEqualTo(Status.OK);
    assertThat(res1.getItemsCount()).isEqualTo(2);
    assertThat(res1.getItems(0).getSequenceId()).isEqualTo(1L);
    assertThat(res1.getItems(1).getSequenceId()).isEqualTo(2L);
    // next_cursor = highest seqId = 2
    assertThat(res1.getNextCursor()).isEqualTo(2L);

    // Fetch with cursor=2 (afterSequence=2) → gets seqId 3
    service.handleFetch(peer, createSignedFetchRequest(2, 2));
    FetchResponse res2 = readFetchResponse();

    assertThat(res2.getStatus()).isEqualTo(Status.OK);
    assertThat(res2.getItemsCount()).isEqualTo(1);
    assertThat(res2.getItems(0).getSequenceId()).isEqualTo(3L);
    assertThat(res2.getNextCursor()).isEqualTo(3L);
  }

  @Test
  public void testFetch_emptyResult_nextCursorEqualsInputCursor() throws Exception {
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse();

    service.handleFetch(peer, createSignedFetchRequest(0));
    FetchResponse res = readFetchResponse();

    assertThat(res.getStatus()).isEqualTo(Status.OK);
    assertThat(res.getItemsCount()).isZero();
    assertThat(res.getNextCursor()).isZero();
  }

  // --- MS02 AC: AckFetch deletes items with sequence_id <= acked_sequence_id ---

  @Test
  public void testAckFetch_deletesAcknowledgedItems() throws Exception {
    byte[] ohId = clientNode.getKademliaId().getBytes();
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse();

    service.depositMessage(ohId, MSG1.getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, MSG2.getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, MSG3.getBytes(StandardCharsets.UTF_8));

    // Fetch to get sequence IDs
    service.handleFetch(peer, createSignedFetchRequest(0));
    FetchResponse fetchRes = readFetchResponse();
    assertThat(fetchRes.getItemsCount()).isEqualTo(3);
    assertThat(fetchRes.getNextCursor()).isEqualTo(3L);

    // AckFetch up to seq 2 — should delete msg1 and msg2
    service.handleAckFetch(peer, createSignedAckFetchRequest(2));
    AckFetchResponse ackRes = readAckFetchResponse();
    assertThat(ackRes.getStatus()).isEqualTo(Status.OK);

    // Fetch again from start — only msg3 (seqId=3) should remain
    service.handleFetch(peer, createSignedFetchRequest(0));
    FetchResponse afterAck = readFetchResponse();
    assertThat(afterAck.getItemsCount()).isEqualTo(1);
    assertThat(afterAck.getItems(0).getPayload().toStringUtf8()).isEqualTo(MSG3);
    assertThat(afterAck.getItems(0).getSequenceId()).isEqualTo(3L);
  }

  @Test
  public void testAckFetch_notFound_returnsNotFound() throws Exception {
    service.handleAckFetch(peer, createSignedAckFetchRequest(1));
    AckFetchResponse res = readAckFetchResponse();
    assertThat(res.getStatus()).isEqualTo(Status.NOT_FOUND);
  }

  // --- MS02 AC: mailbox_overflow flag ---

  @Test
  public void testFetch_mailboxOverflowFlag_setAfterEviction() throws Exception {
    byte[] ohId = clientNode.getKademliaId().getBytes();
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse();

    // Fill mailbox to capacity + 1: the last deposit is rejected (MS02b reject-new)
    for (int i = 0; i < OutboundMailboxStore.MAX_ITEMS_PER_MAILBOX; i++) {
      service.depositMessage(ohId, ("m" + i).getBytes(StandardCharsets.UTF_8));
    }
    OutboundService.DepositResult rejected =
        service.depositMessage(ohId, "one too many".getBytes(StandardCharsets.UTF_8));
    assertThat(rejected).isEqualTo(OutboundService.DepositResult.QUOTA_EXCEEDED);

    service.handleFetch(peer, createSignedFetchRequest(0));
    FetchResponse res = readFetchResponse();

    assertThat(res.getStatus()).isEqualTo(Status.OK);
    assertThat(res.getMailboxOverflow())
        .as("overflow flag should be set after a rejected deposit")
        .isTrue();

    // Second fetch — overflow flag should be cleared
    service.handleFetch(peer, createSignedFetchRequest(0));
    FetchResponse res2 = readFetchResponse();
    assertThat(res2.getMailboxOverflow()).isFalse();
  }

  // --- MS02b AC: per-item size limit surfaces as BAD_REQUEST ---

  @Test
  public void testDeposit_oversizedItem_returnsBadRequest() throws Exception {
    byte[] ohId = clientNode.getKademliaId().getBytes();
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse();

    byte[] oversized = new byte[OutboundMailboxStore.MAX_ITEM_BYTES + 1];
    assertThat(service.depositMessage(ohId, oversized))
        .isEqualTo(OutboundService.DepositResult.BAD_REQUEST);

    // Nothing was stored
    service.handleFetch(peer, createSignedFetchRequest(0));
    assertThat(readFetchResponse().getItemsCount()).isZero();
  }

  // --- MS02b: successful register triggers the OH announce hook ---

  @Test
  public void testRegister_invokesOhRegisteredListener() throws Exception {
    java.util.List<byte[]> announced = new java.util.ArrayList<>();
    service.setOhRegisteredListener(announced::add);

    service.handleRegister(peer, createSignedRegisterRequest());
    assertThat(readRegisterResponse().getStatus()).isEqualTo(Status.OK);

    assertThat(announced).hasSize(1);
    assertThat(announced.get(0)).isEqualTo(clientNode.getKademliaId().getBytes());

    // Failed registers (bad signature) must not trigger the announce hook
    RegisterOhRequest valid = createSignedRegisterRequest();
    RegisterOhRequest tampered =
        valid.toBuilder().setSignature(ByteString.copyFrom(new byte[80])).build();
    service.handleRegister(peer, tampered);
    readRegisterResponse();
    assertThat(announced).hasSize(1);
  }

  /** The announce hook is best-effort: a throwing listener must not break the register flow. */
  @Test
  public void testRegister_throwingListenerDoesNotBreakRegister() throws Exception {
    service.setOhRegisteredListener(
        ohId -> {
          throw new IllegalStateException("announce scheduler down");
        });

    service.handleRegister(peer, createSignedRegisterRequest());

    assertThat(readRegisterResponse().getStatus()).isEqualTo(Status.OK);
    assertThat(handleStore.get(clientNode.getKademliaId().getBytes())).isNotNull();
  }

  // --- MS02b AC: register rate limit per connection ---

  @Test
  public void testRegister_rateLimitPerConnection() throws Exception {
    for (int i = 0; i < OutboundService.REGISTER_LIMIT_PER_WINDOW; i++) {
      service.handleRegister(peer, createSignedRegisterRequest());
      assertThat(readRegisterResponse().getStatus()).isEqualTo(Status.OK);
    }

    // One register above the per-connection window limit → RATE_LIMIT
    service.handleRegister(peer, createSignedRegisterRequest());
    assertThat(readRegisterResponse().getStatus()).isEqualTo(Status.RATE_LIMIT);

    // A different connection is not affected
    Peer otherPeer = new Peer("127.0.0.1", 23456);
    otherPeer.writeBuffer = ByteBuffer.allocate(8192);
    otherPeer.writeBuffer.clear();
    otherPeer.setConnected(true);
    service.handleRegister(otherPeer, createSignedRegisterRequest());
    assertThat(readRegisterResponse(otherPeer).getStatus()).isEqualTo(Status.OK);
  }

  // --- Response readers ---

  private RegisterOhResponse readRegisterResponse() throws Exception {
    return readRegisterResponse(peer);
  }

  private RegisterOhResponse readRegisterResponse(Peer fromPeer) throws Exception {
    fromPeer.writeBuffer.flip();
    byte cmd = fromPeer.writeBuffer.get();
    assertThat(cmd).isEqualTo(Command.OUTBOUND_REGISTER_OH_RES);
    int len = fromPeer.writeBuffer.getInt();
    byte[] data = new byte[len];
    fromPeer.writeBuffer.get(data);
    fromPeer.writeBuffer.compact();
    return RegisterOhResponse.parseFrom(data);
  }

  private FetchResponse readFetchResponse() throws Exception {
    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertThat(cmd).isEqualTo(Command.OUTBOUND_FETCH_RES);
    int len = peer.writeBuffer.getInt();
    byte[] data = new byte[len];
    peer.writeBuffer.get(data);
    peer.writeBuffer.compact();
    return FetchResponse.parseFrom(data);
  }

  private RevokeOhResponse readRevokeResponse() throws Exception {
    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertThat(cmd).isEqualTo(Command.OUTBOUND_REVOKE_OH_RES);
    int len = peer.writeBuffer.getInt();
    byte[] data = new byte[len];
    peer.writeBuffer.get(data);
    peer.writeBuffer.compact();
    return RevokeOhResponse.parseFrom(data);
  }

  private AckFetchResponse readAckFetchResponse() throws Exception {
    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertThat(cmd).isEqualTo(Command.OUTBOUND_ACK_FETCH_RES);
    int len = peer.writeBuffer.getInt();
    byte[] data = new byte[len];
    peer.writeBuffer.get(data);
    peer.writeBuffer.compact();
    return AckFetchResponse.parseFrom(data);
  }

  // --- Request builders ---

  private RegisterOhRequest createSignedRegisterRequest() {
    long now = System.currentTimeMillis();
    long expires = now + 60_000;
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] nonce = randomNonce();

    byte[] signature = signRegister(ohId, expires, now, nonce);

    return RegisterOhRequest.newBuilder()
        .setOhId(ByteString.copyFrom(ohId))
        .setOhAuthPublicKey(ByteString.copyFrom(clientNode.getVerifyKeyBytes()))
        .setRequestedExpiresAt(expires)
        .setTimestampMs(now)
        .setNonce(ByteString.copyFrom(nonce))
        .setSignature(ByteString.copyFrom(signature))
        .build();
  }

  private FetchRequest createSignedFetchRequest(long cursor) {
    return createSignedFetchRequest(cursor, 100);
  }

  private FetchRequest createSignedFetchRequest(long cursor, int limit) {
    long now = System.currentTimeMillis();
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] nonce = randomNonce();

    byte[] signature = signFetch(ohId, limit, cursor, now, nonce);

    return FetchRequest.newBuilder()
        .setOhId(ByteString.copyFrom(ohId))
        .setCursor(cursor)
        .setLimit(limit)
        .setTimestampMs(now)
        .setNonce(ByteString.copyFrom(nonce))
        .setSignature(ByteString.copyFrom(signature))
        .build();
  }

  private RevokeOhRequest createSignedRevokeRequest() {
    long now = System.currentTimeMillis();
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] nonce = randomNonce();

    byte[] signature = signRevoke(ohId, now, nonce);

    return RevokeOhRequest.newBuilder()
        .setOhId(ByteString.copyFrom(ohId))
        .setTimestampMs(now)
        .setNonce(ByteString.copyFrom(nonce))
        .setSignature(ByteString.copyFrom(signature))
        .build();
  }

  private AckFetchRequest createSignedAckFetchRequest(long ackedSeqId) {
    long now = System.currentTimeMillis();
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] nonce = randomNonce();

    byte[] signature = signAckFetch(ohId, ackedSeqId, now, nonce);

    return AckFetchRequest.newBuilder()
        .setOhId(ByteString.copyFrom(ohId))
        .setAckedSequenceId(ackedSeqId)
        .setTimestampMs(now)
        .setNonce(ByteString.copyFrom(nonce))
        .setSignature(ByteString.copyFrom(signature))
        .build();
  }

  // --- Signing helpers ---

  private byte[] randomNonce() {
    byte[] n = new byte[8];
    new SecureRandom().nextBytes(n);
    return n;
  }

  private byte[] signRegister(byte[] ohId, long expires, long ts, byte[] nonce) {
    ByteBuffer buf = ByteBuffer.allocate(2 + ohId.length + 8 + 8 + nonce.length);
    buf.put(OutboundAuth.SIGNING_VERSION_ED25519);
    buf.put(Command.OUTBOUND_REGISTER_OH_REQ);
    buf.put(ohId);
    buf.putLong(expires);
    buf.putLong(ts);
    buf.put(nonce);
    return clientNode.sign(buf.array());
  }

  private byte[] signFetch(byte[] ohId, int limit, long cursor, long ts, byte[] nonce) {
    ByteBuffer buf = ByteBuffer.allocate(2 + ohId.length + 8 + nonce.length + 4 + 8);
    buf.put(OutboundAuth.SIGNING_VERSION_ED25519);
    buf.put(Command.OUTBOUND_FETCH_REQ);
    buf.put(ohId);
    buf.putLong(ts);
    buf.put(nonce);
    buf.putInt(limit);
    buf.putLong(cursor);
    return clientNode.sign(buf.array());
  }

  private byte[] signRevoke(byte[] ohId, long ts, byte[] nonce) {
    ByteBuffer buf = ByteBuffer.allocate(2 + ohId.length + 8 + nonce.length);
    buf.put(OutboundAuth.SIGNING_VERSION_ED25519);
    buf.put(Command.OUTBOUND_REVOKE_OH_REQ);
    buf.put(ohId);
    buf.putLong(ts);
    buf.put(nonce);
    return clientNode.sign(buf.array());
  }

  private byte[] signAckFetch(byte[] ohId, long ackedSeqId, long ts, byte[] nonce) {
    ByteBuffer buf = ByteBuffer.allocate(2 + ohId.length + 8 + 8 + nonce.length);
    buf.put(OutboundAuth.SIGNING_VERSION_ED25519);
    buf.put(Command.OUTBOUND_ACK_FETCH_REQ);
    buf.put(ohId);
    buf.putLong(ackedSeqId);
    buf.putLong(ts);
    buf.put(nonce);
    return clientNode.sign(buf.array());
  }
}
