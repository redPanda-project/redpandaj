package im.redpanda.outbound;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import im.redpanda.core.Command;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.outbound.v1.FetchRequest;
import im.redpanda.outbound.v1.FetchResponse;
import im.redpanda.outbound.v1.RegisterOhRequest;
import im.redpanda.outbound.v1.RegisterOhResponse;
import im.redpanda.outbound.v1.RevokeOhRequest;
import im.redpanda.outbound.v1.RevokeOhResponse;
import im.redpanda.outbound.v1.Status;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import org.junit.Before;
import org.junit.Test;

/**
 * MS01 End-to-End Integration Test: register → deposit → fetch → revoke.
 *
 * <p>Validates the full Outbound Handle lifecycle including message deposit via {@link
 * OutboundService#depositMessage(byte[], byte[])}.
 */
public class OutboundServiceIntegrationTest {

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

    clientNode = new NodeId(NodeId.generateECKeys());
  }

  /** Full lifecycle: register OH → deposit message → fetch message → revoke OH. */
  @Test
  public void testFullLifecycle_Register_Deposit_Fetch_Revoke() throws Exception {
    byte[] ohId = clientNode.getKademliaId().getBytes();

    // 1. Register OH
    RegisterOhRequest regReq = createSignedRegisterRequest();
    service.handleRegister(peer, regReq);

    RegisterOhResponse regRes = readRegisterResponse();
    assertEquals(Status.OK, regRes.getStatus());
    assertTrue(regRes.getExpiresAtMs() > System.currentTimeMillis());

    // 2. Deposit a message via depositMessage
    byte[] payload = "Hello from sender!".getBytes(StandardCharsets.UTF_8);
    boolean deposited = service.depositMessage(ohId, payload);
    assertTrue("Message should be deposited into registered OH", deposited);

    // 3. Fetch the deposited message
    FetchRequest fetchReq = createSignedFetchRequest();
    service.handleFetch(peer, fetchReq);

    FetchResponse fetchRes = readFetchResponse();
    assertEquals(Status.OK, fetchRes.getStatus());
    assertEquals(1, fetchRes.getItemsCount());
    assertEquals("Hello from sender!", fetchRes.getItems(0).getPayload().toStringUtf8());
    assertTrue(fetchRes.getItems(0).getReceivedAtMs() > 0);

    // 4. Revoke the OH
    RevokeOhRequest revokeReq = createSignedRevokeRequest();
    service.handleRevoke(peer, revokeReq);

    RevokeOhResponse revokeRes = readRevokeResponse();
    assertEquals(Status.OK, revokeRes.getStatus());

    // 5. Verify OH is gone
    assertNull(handleStore.get(ohId));
  }

  @Test
  public void testDepositMessage_OhNotRegistered_ReturnsFalse() {
    byte[] unknownOhId = new byte[20];
    new SecureRandom().nextBytes(unknownOhId);
    boolean deposited =
        service.depositMessage(unknownOhId, "test".getBytes(StandardCharsets.UTF_8));
    assertFalse(deposited);
  }

  @Test
  public void testDepositMessage_MultipleMessages_AllFetched() throws Exception {
    // Register
    service.handleRegister(peer, createSignedRegisterRequest());
    readRegisterResponse(); // consume

    byte[] ohId = clientNode.getKademliaId().getBytes();

    // Deposit multiple messages
    service.depositMessage(ohId, "msg1".getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, "msg2".getBytes(StandardCharsets.UTF_8));
    service.depositMessage(ohId, "msg3".getBytes(StandardCharsets.UTF_8));

    // Fetch all
    service.handleFetch(peer, createSignedFetchRequest());
    FetchResponse fetchRes = readFetchResponse();

    assertEquals(Status.OK, fetchRes.getStatus());
    assertEquals(3, fetchRes.getItemsCount());
    assertEquals("msg1", fetchRes.getItems(0).getPayload().toStringUtf8());
    assertEquals("msg2", fetchRes.getItems(1).getPayload().toStringUtf8());
    assertEquals("msg3", fetchRes.getItems(2).getPayload().toStringUtf8());
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
    boolean deposited =
        service.depositMessage(ohId, "late message".getBytes(StandardCharsets.UTF_8));
    assertFalse(deposited);
  }

  // --- Response readers ---

  private RegisterOhResponse readRegisterResponse() throws Exception {
    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertEquals(Command.OUTBOUND_REGISTER_OH_RES, cmd);
    int len = peer.writeBuffer.getInt();
    byte[] data = new byte[len];
    peer.writeBuffer.get(data);
    peer.writeBuffer.compact();
    return RegisterOhResponse.parseFrom(data);
  }

  private FetchResponse readFetchResponse() throws Exception {
    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertEquals(Command.OUTBOUND_FETCH_RES, cmd);
    int len = peer.writeBuffer.getInt();
    byte[] data = new byte[len];
    peer.writeBuffer.get(data);
    peer.writeBuffer.compact();
    return FetchResponse.parseFrom(data);
  }

  private RevokeOhResponse readRevokeResponse() throws Exception {
    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertEquals(Command.OUTBOUND_REVOKE_OH_RES, cmd);
    int len = peer.writeBuffer.getInt();
    byte[] data = new byte[len];
    peer.writeBuffer.get(data);
    peer.writeBuffer.compact();
    return RevokeOhResponse.parseFrom(data);
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
        .setOhAuthPublicKey(ByteString.copyFrom(clientNode.exportPublic()))
        .setRequestedExpiresAt(expires)
        .setTimestampMs(now)
        .setNonce(ByteString.copyFrom(nonce))
        .setSignature(ByteString.copyFrom(signature))
        .build();
  }

  private FetchRequest createSignedFetchRequest() {
    long now = System.currentTimeMillis();
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] nonce = randomNonce();
    int limit = 100;
    long cursor = 0;

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

  // --- Signing helpers ---

  private byte[] randomNonce() {
    byte[] n = new byte[8];
    new SecureRandom().nextBytes(n);
    return n;
  }

  private byte[] signRegister(byte[] ohId, long expires, long ts, byte[] nonce) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      bos.write(Command.OUTBOUND_REGISTER_OH_REQ);
      bos.write(ohId);
      bos.write(com.google.common.primitives.Longs.toByteArray(expires));
      bos.write(com.google.common.primitives.Longs.toByteArray(ts));
      bos.write(nonce);
    } catch (IOException e) {
      throw new AssertionError("ByteArrayOutputStream should not throw IOException", e);
    }
    return clientNode.sign(bos.toByteArray());
  }

  private byte[] signFetch(byte[] ohId, int limit, long cursor, long ts, byte[] nonce) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      bos.write(Command.OUTBOUND_FETCH_REQ);
      bos.write(ohId);
      bos.write(com.google.common.primitives.Longs.toByteArray(ts));
      bos.write(nonce);
      bos.write(com.google.common.primitives.Ints.toByteArray(limit));
      bos.write(com.google.common.primitives.Longs.toByteArray(cursor));
    } catch (IOException e) {
      throw new AssertionError("ByteArrayOutputStream should not throw IOException", e);
    }
    return clientNode.sign(bos.toByteArray());
  }

  private byte[] signRevoke(byte[] ohId, long ts, byte[] nonce) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      bos.write(Command.OUTBOUND_REVOKE_OH_REQ);
      bos.write(ohId);
      bos.write(com.google.common.primitives.Longs.toByteArray(ts));
      bos.write(nonce);
    } catch (IOException e) {
      throw new AssertionError("ByteArrayOutputStream should not throw IOException", e);
    }
    return clientNode.sign(bos.toByteArray());
  }
}
