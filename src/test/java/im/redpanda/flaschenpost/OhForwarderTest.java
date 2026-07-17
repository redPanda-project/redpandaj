package im.redpanda.flaschenpost;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import im.redpanda.core.Command;
import im.redpanda.core.InboundCommandProcessor;
import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
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
 * MS02b acceptance: a FlaschenpostPut whose OH is not registered locally is forwarded across an
 * intermediate node with the {@code oh_id} preserved and deposited into the correct mailbox on the
 * host node.
 */
public class OhForwarderTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  /** Intermediate node A — receives the deposit but does not host the OH. */
  private ServerContext nodeA;

  private InboundCommandProcessor processorA;
  private OutboundHandleStore handleStoreA;
  private OutboundMailboxStore mailboxA;

  /** Host node B — hosts the OH mailbox. */
  private ServerContext nodeB;

  private InboundCommandProcessor processorB;
  private OutboundMailboxStore mailboxB;
  private OutboundHandleStore handleStoreB;

  private byte[] ohId;

  @Before
  public void setUp() {
    nodeA = ServerContext.buildDefaultServerContext();
    handleStoreA = new OutboundHandleStore();
    mailboxA = new OutboundMailboxStore();
    nodeA.setOutboundService(new OutboundService(handleStoreA, mailboxA));
    processorA = new InboundCommandProcessor(nodeA);

    nodeB = ServerContext.buildDefaultServerContext();
    handleStoreB = new OutboundHandleStore();
    mailboxB = new OutboundMailboxStore();
    nodeB.setOutboundService(new OutboundService(handleStoreB, mailboxB));
    processorB = new InboundCommandProcessor(nodeB);

    ohId = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(ohId);

    // OH registered on node B only
    long now = System.currentTimeMillis();
    handleStoreB.put(ohId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));
  }

  /** Builds a [4-byte len][payload] frame as expected by {@code parseCommand}. */
  private static ByteBuffer buildFrame(byte[] payload) {
    ByteBuffer buf = ByteBuffer.allocate(4 + payload.length);
    buf.putInt(payload.length);
    buf.put(payload);
    buf.flip();
    return buf;
  }

  /** Connects a peer object representing node B to node A's peer list. */
  private Peer connectPeerForNodeB() {
    Peer peerB = new Peer("127.0.0.1", 9301, nodeB.getNodeId());
    peerB.setConnected(true);
    peerB.writeBuffer = ByteBuffer.allocate(65536);
    nodeA.getPeerList().add(peerB);
    return peerB;
  }

  @Test
  public void forwardedDeposit_acrossIntermediateNode_keepsOhIdAndLandsInMailbox()
      throws Exception {
    // Node B has announced its OH in the DHT; node A has the (padded, signed) record locally.
    nodeA
        .getKadStoreManager()
        .put(
            OhDht.buildAnnounceContent(
                ohId, nodeB.getNodeId().getKademliaId(), System.currentTimeMillis()));

    Peer peerB = connectPeerForNodeB();

    // A light client deposits at node A (which does NOT host the OH)
    byte[] payload = "cross-node message".getBytes(StandardCharsets.UTF_8);
    FlaschenpostPut put =
        FlaschenpostPut.newBuilder()
            .setContent(ByteString.copyFrom(payload))
            .setOhId(ByteString.copyFrom(ohId))
            .build();
    Peer lightClient = new Peer("127.0.0.1", 9300, nodeA.getNodeId());
    lightClient.setConnected(true);
    nodeA.getPeerList().add(lightClient);

    processorA.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(put.toByteArray()), lightClient);

    // Node A must have forwarded a FLASCHENPOST_PUT to peer B with the oh_id preserved
    ByteBuffer outA = peerB.writeBuffer;
    outA.flip();
    assertThat(outA.hasRemaining()).as("node A must forward to the resolved host node").isTrue();
    assertThat(outA.get()).isEqualTo(Command.FLASCHENPOST_PUT);
    byte[] forwardedBytes = new byte[outA.getInt()];
    outA.get(forwardedBytes);

    FlaschenpostPut forwarded = FlaschenpostPut.parseFrom(forwardedBytes);
    assertThat(forwarded.getOhId().toByteArray())
        .as("Option A: oh_id must survive the forward hop")
        .isEqualTo(ohId);
    assertThat(forwarded.getContent().toByteArray()).isEqualTo(payload);
    assertThat(forwarded.getHopCount()).isEqualTo(1);
    assertThat(forwarded.getWantResponse())
        .as("hop-local response flag must not propagate")
        .isFalse();

    // Node B processes the forwarded packet — the message lands in the correct mailbox
    Peer peerA = new Peer("127.0.0.1", 9302, nodeA.getNodeId());
    peerA.setConnected(true);
    nodeB.getPeerList().add(peerA);
    processorB.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(forwardedBytes), peerA);

    List<MailItem> items = mailboxB.fetchMessages(ohId, 10, 0);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).getPayload().toByteArray()).isEqualTo(payload);
  }

  @Test
  public void forwardedDeposit_atHopLimit_isDroppedNotForwarded() {
    nodeA
        .getKadStoreManager()
        .put(
            OhDht.buildAnnounceContent(
                ohId, nodeB.getNodeId().getKademliaId(), System.currentTimeMillis()));
    Peer peerB = connectPeerForNodeB();

    FlaschenpostPut put =
        FlaschenpostPut.newBuilder()
            .setContent(ByteString.copyFrom(new byte[16]))
            .setOhId(ByteString.copyFrom(ohId))
            .setHopCount(OhForwarder.MAX_HOPS)
            .build();
    Peer sender = new Peer("127.0.0.1", 9303, nodeA.getNodeId());
    sender.setConnected(true);
    nodeA.getPeerList().add(sender);

    processorA.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(put.toByteArray()), sender);

    peerB.writeBuffer.flip();
    assertThat(peerB.writeBuffer.hasRemaining())
        .as("a packet at the hop limit must not be forwarded")
        .isFalse();
  }

  @Test
  public void routeToNode_withoutDirectPeer_dropsWhenNoCloserCandidate() {
    // No peers at all — routing must simply drop without throwing
    OhForwarder.routeToNode(
        nodeA, NodeId.generateWithSimpleKey().getKademliaId(), deposit(new byte[8], null));
  }

  /** Builds a parked-deposit record as OhForwarder.forward would for the given return path. */
  private OhForwarder.PendingDeposit deposit(byte[] content, ReturnPath ackPath) {
    return new OhForwarder.PendingDeposit(
        ohId, content, 0, null, ackPath == null ? null : ackPath.serialize(), ackPath);
  }

  // --- MS06 R-ACK on async drop (first-delivery-latency) ---

  /** Registers an ack-OH mailbox on node A and returns a hop_count=0 return path for it. */
  private ReturnPath registerLocalAckPathA() {
    byte[] ackOhId = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(ackOhId);
    long now = System.currentTimeMillis();
    handleStoreA.put(
        ackOhId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));
    byte[] ackSessionTag = new byte[OutboundService.SESSION_TAG_BYTES];
    RANDOM.nextBytes(ackSessionTag);
    return new ReturnPath(ackOhId, ackSessionTag, List.of());
  }

  /** Fetches the single R-ACK deposited into node A's ack-OH mailbox, asserting exactly one. */
  private im.redpanda.outbound.v1.RoutingAck fetchSingleAckA(ReturnPath ackPath) throws Exception {
    List<MailItem> items = mailboxA.fetchMessages(ackPath.ackOhId(), 10, 0);
    assertThat(items).hasSize(1);
    return im.redpanda.outbound.v1.RoutingAck.parseFrom(items.get(0).getPayload().toByteArray());
  }

  @Test
  public void resolveFailure_finalAttempt_withReturnPath_sendsExactlyOneHandleExpiredAck()
      throws Exception {
    ReturnPath ackPath = registerLocalAckPathA();

    // simulate the async resolve-failure callback of the RETRY directly (a real DHT search would
    // only fail after a randomized network round-trip — the callback is the unit under test)
    OhForwarder.onResolveFailed(nodeA, deposit(new byte[8], ackPath), true);

    im.redpanda.outbound.v1.RoutingAck rAck = fetchSingleAckA(ackPath);
    assertThat(rAck.getStatus()).isEqualTo(RoutingAckSender.STATUS_HANDLE_EXPIRED);
  }

  @Test
  public void resolveFailure_finalAttempt_withoutReturnPath_isPlainDrop() {
    // parseAckPath returns null for absent / empty return paths → no ack, no exception
    assertThat(OhForwarder.parseAckPath(null)).isNull();
    assertThat(OhForwarder.parseAckPath(new byte[0])).isNull();
    // must not throw and must deposit nothing
    OhForwarder.onResolveFailed(nodeA, deposit(new byte[8], null), true);
  }

  @Test
  public void parseAckPath_malformedReturnPath_isNullSoDropStaysSilent() {
    // a structurally invalid return-path block must NOT ack (nobody safe to ack) — plain drop
    assertThat(OhForwarder.parseAckPath(new byte[] {1, 2, 3})).isNull();
    OhForwarder.onResolveFailed(
        nodeA, deposit(new byte[8], OhForwarder.parseAckPath(new byte[] {1, 2, 3})), true);
  }

  @Test
  public void noRoute_withReturnPath_sendsExactlyOneHandleExpiredAck() throws Exception {
    ReturnPath ackPath = registerLocalAckPathA();

    // resolve succeeded but there is no peer to forward toward → async no-route drop must ack
    OhForwarder.routeToNode(
        nodeA, NodeId.generateWithSimpleKey().getKademliaId(), deposit(new byte[8], ackPath));

    im.redpanda.outbound.v1.RoutingAck rAck = fetchSingleAckA(ackPath);
    assertThat(rAck.getStatus()).isEqualTo(RoutingAckSender.STATUS_HANDLE_EXPIRED);
  }

  // --- T32: parked retry instead of first-failure drop ---

  @Test
  public void resolveFailure_firstAttempt_parksForRetryInsteadOfDropping() throws Exception {
    ReturnPath ackPath = registerLocalAckPathA();
    int parkedBefore = OhForwarder.pendingRetries.get();

    OhForwarder.onResolveFailed(nodeA, deposit(new byte[8], ackPath), false);

    // no drop, no ack yet — the deposit is parked for the delayed retry
    assertThat(OhForwarder.pendingRetries.get()).isEqualTo(parkedBefore + 1);
    assertThat(mailboxA.fetchMessages(ackPath.ackOhId(), 10, 0))
        .as("a parked deposit must not ack — only the retry outcome does")
        .isEmpty();
  }

  @Test
  public void retryDeposit_afterLateLocalRegistration_deliversAndAcksStored() throws Exception {
    // T32 regression: the deposit arrived while the OH registration was lost (cut-off
    // connection); the client re-registers on THIS node before the retry fires. The retried
    // deposit must land locally instead of being dropped.
    ReturnPath ackPath = registerLocalAckPathA();
    byte[] payload = "parked message".getBytes(StandardCharsets.UTF_8);
    OhForwarder.PendingDeposit parked = deposit(payload, ackPath);

    // late (re-)registration of the target OH on node A, after the failed first resolve
    long now = System.currentTimeMillis();
    handleStoreA.put(ohId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));

    OhForwarder.retryDeposit(nodeA, parked);

    List<MailItem> items = mailboxA.fetchMessages(ohId, 10, 0);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).getPayload().toByteArray()).isEqualTo(payload);
    im.redpanda.outbound.v1.RoutingAck rAck = fetchSingleAckA(ackPath);
    assertThat(rAck.getStatus()).isEqualTo(RoutingAckSender.STATUS_STORED);
  }

  @Test
  public void resolveFailure_retryBufferFull_dropsWithHandleExpiredAck() throws Exception {
    ReturnPath ackPath = registerLocalAckPathA();
    int parkedBefore = OhForwarder.pendingRetries.get();
    // fill the bounded buffer artificially (way past the cap so concurrent decrements from other
    // tests' scheduled retries cannot un-fill it)
    OhForwarder.pendingRetries.set(OhForwarder.MAX_PENDING_RETRIES + 1000);
    try {
      OhForwarder.onResolveFailed(nodeA, deposit(new byte[8], ackPath), false);
    } finally {
      OhForwarder.pendingRetries.set(parkedBefore);
    }

    // buffer full → immediate final drop with exactly one HANDLE_EXPIRED ack
    im.redpanda.outbound.v1.RoutingAck rAck = fetchSingleAckA(ackPath);
    assertThat(rAck.getStatus()).isEqualTo(RoutingAckSender.STATUS_HANDLE_EXPIRED);
  }

  @Test
  public void routeToNode_targetIsSelf_depositsLocallyInsteadOfDropping() throws Exception {
    // resolve returned OUR node id (the OH registered here between the caller's NOT_FOUND check
    // and the resolve callback) — there is never a route to ourselves, so this must deposit
    ReturnPath ackPath = registerLocalAckPathA();
    byte[] payload = "self-resolved message".getBytes(StandardCharsets.UTF_8);
    long now = System.currentTimeMillis();
    handleStoreA.put(ohId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));

    OhForwarder.routeToNode(nodeA, nodeA.getNonce(), deposit(payload, ackPath));

    List<MailItem> items = mailboxA.fetchMessages(ohId, 10, 0);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).getPayload().toByteArray()).isEqualTo(payload);
    im.redpanda.outbound.v1.RoutingAck rAck = fetchSingleAckA(ackPath);
    assertThat(rAck.getStatus()).isEqualTo(RoutingAckSender.STATUS_STORED);
  }

  @Test
  public void routeToNode_targetIsSelf_unknownOh_acksHandleExpired() throws Exception {
    // announce points at us but the OH is not registered here (expired/revoked) — final drop
    ReturnPath ackPath = registerLocalAckPathA();

    OhForwarder.routeToNode(nodeA, nodeA.getNonce(), deposit(new byte[8], ackPath));

    assertThat(mailboxA.fetchMessages(ohId, 10, 0)).isEmpty();
    im.redpanda.outbound.v1.RoutingAck rAck = fetchSingleAckA(ackPath);
    assertThat(rAck.getStatus()).isEqualTo(RoutingAckSender.STATUS_HANDLE_EXPIRED);
  }

  @Test
  public void hopLimitDrop_doesNotAck_soCallerAcksWithoutDoubleAck() throws Exception {
    // a valid, LOCALLY resolvable ack path so any accidental ack would land where we can see it
    ReturnPath ackPath = registerLocalAckPathA();

    boolean accepted =
        OhForwarder.forward(
            nodeA, ohId, new byte[8], OhForwarder.MAX_HOPS, null, ackPath.serialize());

    // forward must report the drop (false) so the caller acks; it must NOT ack here itself
    assertThat(accepted).isFalse();
    assertThat(mailboxA.fetchMessages(ackPath.ackOhId(), 10, 0))
        .as("hop-limit drop must not ack — the caller does")
        .isEmpty();
  }
}
