package im.redpanda.outbound;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.outbound.v1.RegisterOhRequest;
import org.junit.Before;
import org.junit.Test;

public class OutboundIntegrationTest {

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
    peer.writeBuffer = java.nio.ByteBuffer.allocate(4096);
    peer.writeBuffer.flip();
    peer.writeBuffer.clear();
    peer.setConnected(true);

    // Generate valid client identity
    clientNode = new NodeId(NodeId.generateECKeys());
  }

  @Test
  public void testRegister_ValidRequest_Success() throws Exception {
    RegisterOhRequest req = createSignedRegisterRequest();
    service.handleRegister(peer, req);

    // Verify response
    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertEquals(im.redpanda.core.Command.OUTBOUND_REGISTER_OH_RES, cmd);
    int len = peer.writeBuffer.getInt();
    byte[] payload = new byte[len];
    peer.writeBuffer.get(payload);

    im.redpanda.outbound.v1.RegisterOhResponse res =
        im.redpanda.outbound.v1.RegisterOhResponse.parseFrom(payload);

    assertEquals(im.redpanda.outbound.v1.Status.OK, res.getStatus());
  }

  @Test
  public void testFetch_Success() throws Exception {
    // 1. Register first
    RegisterOhRequest regReq = createSignedRegisterRequest();
    service.handleRegister(peer, regReq);
    peer.writeBuffer.clear(); // Clear register response

    // 2. Add some mail manually to store for testing fetch
    byte[] ohId = clientNode.getKademliaId().getBytes();
    mailboxStore.addMessage(
        ohId,
        im.redpanda.outbound.v1.MailItem.newBuilder()
            .setPayload(com.google.protobuf.ByteString.copyFromUtf8("TestMsg"))
            .build());

    // 3. Fetch
    im.redpanda.outbound.v1.FetchRequest fetchReq = createSignedFetchRequest();
    service.handleFetch(peer, fetchReq);

    // 4. Verify
    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertEquals(im.redpanda.core.Command.OUTBOUND_FETCH_RES, cmd);
    int len = peer.writeBuffer.getInt();
    byte[] payload = new byte[len];
    peer.writeBuffer.get(payload);

    im.redpanda.outbound.v1.FetchResponse res =
        im.redpanda.outbound.v1.FetchResponse.parseFrom(payload);

    assertEquals(im.redpanda.outbound.v1.Status.OK, res.getStatus());
    assertEquals(1, res.getItemsCount());
    assertEquals("TestMsg", res.getItems(0).getPayload().toStringUtf8());
  }

  @Test
  public void testRevoke_Success() throws Exception {
    // 1. Register first
    RegisterOhRequest regReq = createSignedRegisterRequest();
    service.handleRegister(peer, regReq);
    peer.writeBuffer.clear();

    // 2. Revoke
    im.redpanda.outbound.v1.RevokeOhRequest revokeReq = createSignedRevokeRequest();
    service.handleRevoke(peer, revokeReq);

    // 3. Verify Response
    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertEquals(im.redpanda.core.Command.OUTBOUND_REVOKE_OH_RES, cmd);
    int len = peer.writeBuffer.getInt();
    byte[] payload = new byte[len];
    peer.writeBuffer.get(payload);

    im.redpanda.outbound.v1.RevokeOhResponse res =
        im.redpanda.outbound.v1.RevokeOhResponse.parseFrom(payload);

    assertEquals(im.redpanda.outbound.v1.Status.OK, res.getStatus());

    // 4. Verify store is empty
    assertNull(handleStore.get(clientNode.getKademliaId().getBytes()));
  }

  @Test
  public void testFetch_NotFound() throws Exception {
    im.redpanda.outbound.v1.FetchRequest fetchReq = createSignedFetchRequest();
    service.handleFetch(peer, fetchReq);

    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertEquals(im.redpanda.core.Command.OUTBOUND_FETCH_RES, cmd);
    int len = peer.writeBuffer.getInt();
    byte[] payload = new byte[len];
    peer.writeBuffer.get(payload);

    im.redpanda.outbound.v1.FetchResponse res =
        im.redpanda.outbound.v1.FetchResponse.parseFrom(payload);

    assertEquals(im.redpanda.outbound.v1.Status.NOT_FOUND, res.getStatus());
  }

  @Test
  public void testRevoke_NotFound() throws Exception {
    im.redpanda.outbound.v1.RevokeOhRequest revokeReq = createSignedRevokeRequest();
    service.handleRevoke(peer, revokeReq);

    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertEquals(im.redpanda.core.Command.OUTBOUND_REVOKE_OH_RES, cmd);
    int len = peer.writeBuffer.getInt();
    byte[] payload = new byte[len];
    peer.writeBuffer.get(payload);

    im.redpanda.outbound.v1.RevokeOhResponse res =
        im.redpanda.outbound.v1.RevokeOhResponse.parseFrom(payload);

    assertEquals(im.redpanda.outbound.v1.Status.NOT_FOUND, res.getStatus());
  }

  // --- Helpers ---

  private RegisterOhRequest createSignedRegisterRequest() {
    long now = System.currentTimeMillis();
    long expires = now + 60000;
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] nonce = getRandomNonce();

    byte[] signature = signRegister(ohId, expires, now, nonce);

    return RegisterOhRequest.newBuilder()
        .setOhId(com.google.protobuf.ByteString.copyFrom(ohId))
        .setOhAuthPublicKey(com.google.protobuf.ByteString.copyFrom(clientNode.exportPublic()))
        .setRequestedExpiresAt(expires)
        .setTimestampMs(now)
        .setNonce(com.google.protobuf.ByteString.copyFrom(nonce))
        .setSignature(com.google.protobuf.ByteString.copyFrom(signature))
        .build();
  }

  private im.redpanda.outbound.v1.FetchRequest createSignedFetchRequest() {
    long now = System.currentTimeMillis();
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] nonce = getRandomNonce();
    int limit = 10;
    long cursor = 0;

    byte[] signature = signFetch(ohId, limit, cursor, now, nonce);

    return im.redpanda.outbound.v1.FetchRequest.newBuilder()
        .setOhId(com.google.protobuf.ByteString.copyFrom(ohId))
        .setCursor(cursor)
        .setLimit(limit)
        .setTimestampMs(now)
        .setNonce(com.google.protobuf.ByteString.copyFrom(nonce))
        .setSignature(com.google.protobuf.ByteString.copyFrom(signature))
        .build();
  }

  private im.redpanda.outbound.v1.RevokeOhRequest createSignedRevokeRequest() {
    long now = System.currentTimeMillis();
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] nonce = getRandomNonce();

    byte[] signature = signRevoke(ohId, now, nonce);

    return im.redpanda.outbound.v1.RevokeOhRequest.newBuilder()
        .setOhId(com.google.protobuf.ByteString.copyFrom(ohId))
        .setTimestampMs(now)
        .setNonce(com.google.protobuf.ByteString.copyFrom(nonce))
        .setSignature(com.google.protobuf.ByteString.copyFrom(signature))
        .build();
  }

  private byte[] getRandomNonce() {
    byte[] n = new byte[8];
    new java.util.Random().nextBytes(n);
    return n;
  }

  // Use Guava or manual concat
  private byte[] signRegister(byte[] ohId, long expires, long ts, byte[] nonce) {
    java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
    try {
      bos.write(im.redpanda.core.Command.OUTBOUND_REGISTER_OH_REQ);
      bos.write(ohId);
      bos.write(com.google.common.primitives.Longs.toByteArray(expires));
      bos.write(com.google.common.primitives.Longs.toByteArray(ts));
      bos.write(nonce);
    } catch (Exception e) {
      // Ignored for testing; signature generation failures will fail test elsewhere
    }
    return clientNode.sign(bos.toByteArray());
  }

  private byte[] signFetch(byte[] ohId, int limit, long cursor, long ts, byte[] nonce) {
    java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
    try {
      bos.write(im.redpanda.core.Command.OUTBOUND_FETCH_REQ);
      bos.write(ohId);
      bos.write(com.google.common.primitives.Longs.toByteArray(ts));
      bos.write(nonce);
      bos.write(com.google.common.primitives.Ints.toByteArray(limit));
      bos.write(com.google.common.primitives.Longs.toByteArray(cursor));

    } catch (Exception e) {
      // Ignored for testing helper
    }
    // I will rewrite the whole method body to be safe and match OutboundService.
    return clientNode.sign(bos.toByteArray());
  }

  private byte[] signRevoke(byte[] ohId, long ts, byte[] nonce) {
    java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
    try {
      bos.write(im.redpanda.core.Command.OUTBOUND_REVOKE_OH_REQ);
      bos.write(ohId);
      bos.write(com.google.common.primitives.Longs.toByteArray(ts));
      bos.write(nonce);
    } catch (Exception e) {
      // Ignored for testing helper
    }
    return clientNode.sign(bos.toByteArray());
  }

  @Test
  public void testRegister_InvalidSignature_Fails() throws Exception {
    RegisterOhRequest req = createSignedRegisterRequest();
    // Tamper with signature
    byte[] badSig = req.getSignature().toByteArray();
    badSig[0] ^= 1;
    RegisterOhRequest tampered =
        req.toBuilder().setSignature(com.google.protobuf.ByteString.copyFrom(badSig)).build();

    service.handleRegister(peer, tampered);

    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertEquals(im.redpanda.core.Command.OUTBOUND_REGISTER_OH_RES, cmd);
    int len = peer.writeBuffer.getInt();
    byte[] payload = new byte[len];
    peer.writeBuffer.get(payload);

    im.redpanda.outbound.v1.RegisterOhResponse res =
        im.redpanda.outbound.v1.RegisterOhResponse.parseFrom(payload);

    assertEquals(im.redpanda.outbound.v1.Status.INVALID_SIGNATURE, res.getStatus());
  }

  @Test
  public void testFetch_InvalidSignature_Fails() throws Exception {
    // 1. Register first
    RegisterOhRequest regReq = createSignedRegisterRequest();
    service.handleRegister(peer, regReq);
    peer.writeBuffer.clear();

    // 2. Fetch with bad signature
    im.redpanda.outbound.v1.FetchRequest fetchReq = createSignedFetchRequest();
    byte[] badSig = fetchReq.getSignature().toByteArray();
    badSig[0] ^= 1;
    im.redpanda.outbound.v1.FetchRequest tampered =
        fetchReq.toBuilder().setSignature(com.google.protobuf.ByteString.copyFrom(badSig)).build();

    service.handleFetch(peer, tampered);

    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertEquals(im.redpanda.core.Command.OUTBOUND_FETCH_RES, cmd);
    int len = peer.writeBuffer.getInt();
    byte[] payload = new byte[len];
    peer.writeBuffer.get(payload);

    im.redpanda.outbound.v1.FetchResponse res =
        im.redpanda.outbound.v1.FetchResponse.parseFrom(payload);

    assertEquals(im.redpanda.outbound.v1.Status.INVALID_SIGNATURE, res.getStatus());
  }
}
