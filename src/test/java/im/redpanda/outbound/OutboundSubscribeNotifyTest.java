package im.redpanda.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import im.redpanda.core.Command;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.outbound.v1.Notify;
import im.redpanda.outbound.v1.RegisterOhRequest;
import im.redpanda.outbound.v1.Status;
import im.redpanda.outbound.v1.SubscribeRequest;
import im.redpanda.outbound.v1.SubscribeResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import org.junit.Before;
import org.junit.Test;

/**
 * T38 Connection-Notify: subscribe ownership proof (valid / bad signature / replay / NOT_FOUND),
 * one-way Notify on deposit into a subscribed vs. non-subscribed mailbox, disconnect cleanup,
 * idempotent re-subscribe, and multiple OHs per connection.
 */
public class OutboundSubscribeNotifyTest {

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

    peer = newConnectedPeer(12345);
    clientNode = NodeId.generateWithSimpleKey();
  }

  private static Peer newConnectedPeer(int port) {
    Peer p = new Peer("127.0.0.1", port);
    p.writeBuffer = ByteBuffer.allocate(8192);
    p.writeBuffer.clear();
    p.setConnected(true);
    return p;
  }

  private void registerOh() throws Exception {
    service.handleRegister(peer, createSignedRegisterRequest());
    // consume the RegisterOhResponse so the buffer only holds later commands
    drainBuffer(peer);
  }

  // --- Subscribe auth ---

  @Test
  public void subscribe_validSignature_returnsOkAndBinds() throws Exception {
    registerOh();
    service.handleSubscribe(peer, createSignedSubscribeRequest());
    assertThat(readSubscribeResponse().getStatus()).isEqualTo(Status.OK);

    // A deposit into the subscribed mailbox now emits exactly one Notify(oh_id)
    service.depositMessage(ohId(), "hi".getBytes(StandardCharsets.UTF_8));
    Notify notify = readNotify();
    assertThat(notify.getOhId().toByteArray()).isEqualTo(ohId());
    assertNoMoreData(peer);
  }

  @Test
  public void subscribe_badSignature_returnsInvalidSignature_noNotify() throws Exception {
    registerOh();
    SubscribeRequest tampered =
        createSignedSubscribeRequest().toBuilder()
            .setSignature(ByteString.copyFrom(new byte[64]))
            .build();
    service.handleSubscribe(peer, tampered);
    assertThat(readSubscribeResponse().getStatus()).isEqualTo(Status.INVALID_SIGNATURE);

    // Not subscribed → deposit produces no Notify
    service.depositMessage(ohId(), "hi".getBytes(StandardCharsets.UTF_8));
    assertNoMoreData(peer);
  }

  @Test
  public void subscribe_replayedNonce_returnsReplay() throws Exception {
    registerOh();
    SubscribeRequest req = createSignedSubscribeRequest();
    service.handleSubscribe(peer, req);
    assertThat(readSubscribeResponse().getStatus()).isEqualTo(Status.OK);

    // Exact same request (same nonce) → replay
    service.handleSubscribe(peer, req);
    assertThat(readSubscribeResponse().getStatus()).isEqualTo(Status.REPLAY);
  }

  @Test
  public void subscribe_unknownOhId_returnsNotFound() throws Exception {
    // No register → handle store has no record for this oh_id
    service.handleSubscribe(peer, createSignedSubscribeRequest());
    assertThat(readSubscribeResponse().getStatus()).isEqualTo(Status.NOT_FOUND);
  }

  // --- Notify opt-in semantics ---

  @Test
  public void deposit_intoNonSubscribedMailbox_sendsNoNotify() throws Exception {
    registerOh(); // registered but never subscribed
    service.depositMessage(ohId(), "hi".getBytes(StandardCharsets.UTF_8));
    assertNoMoreData(peer);
  }

  @Test
  public void resubscribe_isIdempotent_stillOneNotify() throws Exception {
    registerOh();
    service.handleSubscribe(peer, createSignedSubscribeRequest());
    assertThat(readSubscribeResponse().getStatus()).isEqualTo(Status.OK);
    service.handleSubscribe(peer, createSignedSubscribeRequest());
    assertThat(readSubscribeResponse().getStatus()).isEqualTo(Status.OK);

    service.depositMessage(ohId(), "hi".getBytes(StandardCharsets.UTF_8));
    assertThat(readNotify().getOhId().toByteArray()).isEqualTo(ohId());
    assertNoMoreData(peer); // exactly one notify, not two
  }

  @Test
  public void disconnect_removesSubscription_noNotify() throws Exception {
    registerOh();
    service.handleSubscribe(peer, createSignedSubscribeRequest());
    assertThat(readSubscribeResponse().getStatus()).isEqualTo(Status.OK);

    // Peer disconnects: deposit must not notify (and the binding is pruned lazily)
    peer.setConnected(false);
    service.depositMessage(ohId(), "hi".getBytes(StandardCharsets.UTF_8));
    peer.writeBuffer.flip();
    assertThat(peer.writeBuffer.hasRemaining()).as("no Notify to a disconnected peer").isFalse();
  }

  @Test
  public void multipleOhsPerConnection_eachNotified() throws Exception {
    // OH #1 (the shared clientNode) + a second OH on the same connection
    registerOh();
    service.handleSubscribe(peer, createSignedSubscribeRequest());
    assertThat(readSubscribeResponse().getStatus()).isEqualTo(Status.OK);

    NodeId other = NodeId.generateWithSimpleKey();
    byte[] otherOhId = other.getKademliaId().getBytes();
    service.handleRegister(peer, createSignedRegisterRequest(other));
    drainBuffer(peer);
    service.handleSubscribe(peer, createSignedSubscribeRequest(other));
    assertThat(readSubscribeResponse().getStatus()).isEqualTo(Status.OK);

    service.depositMessage(otherOhId, "b".getBytes(StandardCharsets.UTF_8));
    assertThat(readNotify().getOhId().toByteArray()).isEqualTo(otherOhId);

    service.depositMessage(ohId(), "a".getBytes(StandardCharsets.UTF_8));
    assertThat(readNotify().getOhId().toByteArray()).isEqualTo(ohId());
  }

  // --- helpers ---

  private byte[] ohId() {
    return clientNode.getKademliaId().getBytes();
  }

  private static void drainBuffer(Peer p) {
    p.writeBuffer.clear();
  }

  private static void assertNoMoreData(Peer p) {
    p.writeBuffer.flip();
    boolean remaining = p.writeBuffer.hasRemaining();
    p.writeBuffer.compact();
    assertThat(remaining).as("no extra command written").isFalse();
  }

  private SubscribeResponse readSubscribeResponse() throws Exception {
    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertThat(cmd).isEqualTo(Command.OUTBOUND_SUBSCRIBE_RES);
    int len = peer.writeBuffer.getInt();
    byte[] data = new byte[len];
    peer.writeBuffer.get(data);
    peer.writeBuffer.compact();
    return SubscribeResponse.parseFrom(data);
  }

  private Notify readNotify() throws Exception {
    peer.writeBuffer.flip();
    byte cmd = peer.writeBuffer.get();
    assertThat(cmd).isEqualTo(Command.OUTBOUND_NOTIFY);
    int len = peer.writeBuffer.getInt();
    byte[] data = new byte[len];
    peer.writeBuffer.get(data);
    peer.writeBuffer.compact();
    return Notify.parseFrom(data);
  }

  // --- request builders / signing ---

  private RegisterOhRequest createSignedRegisterRequest() {
    return createSignedRegisterRequest(clientNode);
  }

  private RegisterOhRequest createSignedRegisterRequest(NodeId node) {
    long now = System.currentTimeMillis();
    long expires = now + 60_000;
    byte[] id = node.getKademliaId().getBytes();
    byte[] nonce = randomNonce();

    ByteBuffer buf = ByteBuffer.allocate(2 + id.length + 8 + 8 + nonce.length);
    buf.put(OutboundAuth.SIGNING_VERSION_ED25519);
    buf.put(Command.OUTBOUND_REGISTER_OH_REQ);
    buf.put(id);
    buf.putLong(expires);
    buf.putLong(now);
    buf.put(nonce);

    return RegisterOhRequest.newBuilder()
        .setOhId(ByteString.copyFrom(id))
        .setOhAuthPublicKey(ByteString.copyFrom(node.getVerifyKeyBytes()))
        .setRequestedExpiresAt(expires)
        .setTimestampMs(now)
        .setNonce(ByteString.copyFrom(nonce))
        .setSignature(ByteString.copyFrom(node.sign(buf.array())))
        .build();
  }

  private SubscribeRequest createSignedSubscribeRequest() {
    return createSignedSubscribeRequest(clientNode);
  }

  private SubscribeRequest createSignedSubscribeRequest(NodeId node) {
    long now = System.currentTimeMillis();
    byte[] id = node.getKademliaId().getBytes();
    byte[] nonce = randomNonce();

    ByteBuffer buf = ByteBuffer.allocate(2 + id.length + 8 + nonce.length);
    buf.put(OutboundAuth.SIGNING_VERSION_ED25519);
    buf.put(Command.OUTBOUND_SUBSCRIBE_REQ);
    buf.put(id);
    buf.putLong(now);
    buf.put(nonce);

    return SubscribeRequest.newBuilder()
        .setOhId(ByteString.copyFrom(id))
        .setTimestampMs(now)
        .setNonce(ByteString.copyFrom(nonce))
        .setSignature(ByteString.copyFrom(node.sign(buf.array())))
        .build();
  }

  private static byte[] randomNonce() {
    byte[] n = new byte[8];
    new SecureRandom().nextBytes(n);
    return n;
  }
}
