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
 * MS05 acceptance: a reverse-garlic reply — built by the responder along the return-path hops the
 * recipient chose — traverses three relays exactly like an MS04 forward packet; the last relay
 * peels the {@code CMD_DELIVER_TAGGED} layer and deposits payload <em>plus 16-byte session tag</em>
 * into the OH mailbox. Relays on the reverse path run unmodified MS04 logic (no special reverse
 * handling); the session tag stays inside the innermost encrypted layer.
 *
 * <p>Plus the negative paths: dedup/replay of a reply packet, truncated tagged layers, invalid
 * payload length, and the MS02b fallback preserving the session tag when the final hop is not the
 * OH host.
 */
public class ReverseGarlicRouterTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  private ServerContext hop1;
  private ServerContext hop2;
  private ServerContext hop3;
  private InboundCommandProcessor processor1;
  private InboundCommandProcessor processor2;
  private InboundCommandProcessor processor3;
  private OutboundMailboxStore aliceMailbox;

  /** Alice's OH, registered on hop3 (the final station of the return path). */
  private byte[] aliceOhId;

  /** The session tag Alice chose for this conversation's reply path. */
  private byte[] sessionTag;

  @Before
  public void setUp() {
    hop1 = ServerContext.buildDefaultServerContext();
    hop1.setOutboundService(
        new OutboundService(new OutboundHandleStore(), new OutboundMailboxStore()));
    processor1 = new InboundCommandProcessor(hop1);

    hop2 = ServerContext.buildDefaultServerContext();
    hop2.setOutboundService(
        new OutboundService(new OutboundHandleStore(), new OutboundMailboxStore()));
    processor2 = new InboundCommandProcessor(hop2);

    hop3 = ServerContext.buildDefaultServerContext();
    OutboundHandleStore handleStore3 = new OutboundHandleStore();
    aliceMailbox = new OutboundMailboxStore();
    hop3.setOutboundService(new OutboundService(handleStore3, aliceMailbox));
    processor3 = new InboundCommandProcessor(hop3);

    aliceOhId = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(aliceOhId);
    sessionTag = new byte[FlaschenpostV2.SESSION_TAG_LEN];
    RANDOM.nextBytes(sessionTag);

    long now = System.currentTimeMillis();
    handleStore3.put(
        aliceOhId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));
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

  /** A connected sender peer (the packet origin, e.g. the replying light client). */
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

  /** Builds the innermost CMD_DELIVER_TAGGED plaintext (oh_id + session_tag + payload). */
  private byte[] buildTaggedDeliverPlaintext(byte[] ohId, byte[] tag, byte[] payload) {
    ByteBuffer deliver =
        ByteBuffer.allocate(1 + KademliaId.ID_LENGTH_BYTES + tag.length + 4 + payload.length);
    deliver.put(FlaschenpostV2.CMD_DELIVER_TAGGED);
    deliver.put(ohId);
    deliver.put(tag);
    deliver.putInt(payload.length);
    deliver.put(payload);
    return deliver.array();
  }

  /**
   * Builds the reply packet exactly like the responder's client will: standard MS04 layer
   * construction along the return-path hops, with a CMD_DELIVER_TAGGED innermost layer.
   */
  private byte[] buildReplyPacket(byte[] payload, ServerContext... hops) throws Exception {
    ServerContext last = hops[hops.length - 1];
    byte[] body =
        FlaschenpostV2.encryptLayer(
            last.getNodeId().getEncryptionPubKey(),
            last.getNonce(),
            buildTaggedDeliverPlaintext(aliceOhId, sessionTag, payload));

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
  public void replyPacket_traversesThreeRelays_andIsDepositedWithSessionTag() throws Exception {
    Peer peer1To2 = connect(hop1, hop2, 9501);
    Peer peer2To3 = connect(hop2, hop3, 9502);

    byte[] payload = "reverse garlic reply".getBytes(StandardCharsets.UTF_8);
    byte[] packet = buildReplyPacket(payload, hop1, hop2, hop3);
    int originalPacketId = FlaschenpostV2.parse(packet).getPacketId();

    // return-path relay 1 behaves exactly like an MS04 forward relay
    processor1.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(hop1, 9500));
    byte[] toHop2 = readV2Frame(peer1To2);
    assertThat(toHop2).hasSize(FlaschenpostV2.PACKET_SIZE);
    FlaschenpostV2 parsed2 = FlaschenpostV2.parse(toHop2);
    assertThat(parsed2.getNextHop()).isEqualTo(hop2.getNonce());
    assertThat(parsed2.getPacketId()).isNotEqualTo(originalPacketId);

    // return-path relay 2
    processor2.parseCommand(Command.FLASCHENPOST_V2, buildFrame(toHop2), sender(hop2, 9503));
    byte[] toHop3 = readV2Frame(peer2To3);
    assertThat(toHop3).hasSize(FlaschenpostV2.PACKET_SIZE);
    assertThat(FlaschenpostV2.parse(toHop3).getNextHop()).isEqualTo(hop3.getNonce());

    // final station peels CMD_DELIVER_TAGGED and deposits payload + session tag
    processor3.parseCommand(Command.FLASCHENPOST_V2, buildFrame(toHop3), sender(hop3, 9504));
    List<MailItem> items = aliceMailbox.fetchMessages(aliceOhId, 10, 0);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).getPayload().toByteArray()).isEqualTo(payload);
    assertThat(items.get(0).getSessionTag().toByteArray())
        .as("the session tag must be stored with the mail item")
        .isEqualTo(sessionTag);
  }

  @Test
  public void untaggedDeliver_keepsWorking_withEmptySessionTag() throws Exception {
    // backward compatibility: an MS04 CMD_DELIVER (no tag) still deposits, tag stays empty
    byte[] payload = "plain ms04 deliver".getBytes(StandardCharsets.UTF_8);
    ByteBuffer deliver = ByteBuffer.allocate(1 + KademliaId.ID_LENGTH_BYTES + 4 + payload.length);
    deliver.put(FlaschenpostV2.CMD_DELIVER);
    deliver.put(aliceOhId);
    deliver.putInt(payload.length);
    deliver.put(payload);
    byte[] body =
        FlaschenpostV2.encryptLayer(
            hop3.getNodeId().getEncryptionPubKey(), hop3.getNonce(), deliver.array());
    byte[] packet = FlaschenpostV2.buildPacket(RANDOM.nextInt(), hop3.getNonce(), body);

    processor3.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(hop3, 9510));

    List<MailItem> items = aliceMailbox.fetchMessages(aliceOhId, 10, 0);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).getPayload().toByteArray()).isEqualTo(payload);
    assertThat(items.get(0).getSessionTag().isEmpty()).isTrue();
  }

  @Test
  public void replayedReplyPacket_isDroppedByDedup() throws Exception {
    Peer peer1To2 = connect(hop1, hop2, 9521);
    byte[] packet = buildReplyPacket(new byte[16], hop1, hop2, hop3);

    processor1.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(hop1, 9520));
    assertThat(readV2Frame(peer1To2)).hasSize(FlaschenpostV2.PACKET_SIZE);

    // a captured reply packet replayed against the entry hop must be dropped (packet_id dedup)
    processor1.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(hop1, 9522));
    peer1To2.writeBuffer.flip();
    assertThat(peer1To2.writeBuffer.hasRemaining())
        .as("a replayed reply packet must not be forwarded again")
        .isFalse();
  }

  @Test
  public void taggedDeliverLayer_tooShortForTag_isDroppedSilently() throws Exception {
    // CMD_DELIVER_TAGGED needs 1 + 20 + 16 + 4 = 41 plaintext bytes; this layer only carries
    // the untagged minimum (25) and must be dropped without crash or deposit
    ByteBuffer deliver = ByteBuffer.allocate(1 + KademliaId.ID_LENGTH_BYTES + 4);
    deliver.put(FlaschenpostV2.CMD_DELIVER_TAGGED);
    deliver.put(aliceOhId);
    deliver.putInt(0);
    byte[] body =
        FlaschenpostV2.encryptLayer(
            hop3.getNodeId().getEncryptionPubKey(), hop3.getNonce(), deliver.array());
    byte[] packet = FlaschenpostV2.buildPacket(RANDOM.nextInt(), hop3.getNonce(), body);

    processor3.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(hop3, 9530));

    assertThat(aliceMailbox.fetchMessages(aliceOhId, 10, 0)).isEmpty();
  }

  @Test
  public void taggedDeliver_invalidPayloadLength_isDropped() throws Exception {
    // payload_len claims more bytes than the plaintext contains — must be rejected
    ByteBuffer deliver =
        ByteBuffer.allocate(1 + KademliaId.ID_LENGTH_BYTES + FlaschenpostV2.SESSION_TAG_LEN + 4);
    deliver.put(FlaschenpostV2.CMD_DELIVER_TAGGED);
    deliver.put(aliceOhId);
    deliver.put(sessionTag);
    deliver.putInt(500);
    byte[] body =
        FlaschenpostV2.encryptLayer(
            hop3.getNodeId().getEncryptionPubKey(), hop3.getNonce(), deliver.array());
    byte[] packet = FlaschenpostV2.buildPacket(RANDOM.nextInt(), hop3.getNonce(), body);

    processor3.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(hop3, 9540));

    assertThat(aliceMailbox.fetchMessages(aliceOhId, 10, 0)).isEmpty();
  }

  @Test
  public void taggedDeliverForRemoteOh_fallsBackToMs02b_preservingSessionTag() throws Exception {
    // the final return-path hop (hop1) does not host Alice's OH; it must forward a
    // FlaschenpostPut that carries oh_id, payload AND the session tag toward the host node
    byte[] remoteOhId = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(remoteOhId);
    hop1.getKadStoreManager()
        .put(OhDht.buildAnnounceContent(remoteOhId, hop2.getNonce(), System.currentTimeMillis()));
    Peer peer1To2 = connect(hop1, hop2, 9551);

    byte[] payload = "reply for remote oh".getBytes(StandardCharsets.UTF_8);
    byte[] body =
        FlaschenpostV2.encryptLayer(
            hop1.getNodeId().getEncryptionPubKey(),
            hop1.getNonce(),
            buildTaggedDeliverPlaintext(remoteOhId, sessionTag, payload));
    byte[] packet = FlaschenpostV2.buildPacket(RANDOM.nextInt(), hop1.getNonce(), body);

    processor1.parseCommand(Command.FLASCHENPOST_V2, buildFrame(packet), sender(hop1, 9550));

    ByteBuffer out = peer1To2.writeBuffer;
    out.flip();
    assertThat(out.hasRemaining()).as("MS02b forward expected").isTrue();
    assertThat(out.get()).isEqualTo(Command.FLASCHENPOST_PUT);
    byte[] forwardedBytes = new byte[out.getInt()];
    out.get(forwardedBytes);
    FlaschenpostPut forwarded = FlaschenpostPut.parseFrom(forwardedBytes);
    assertThat(forwarded.getOhId().toByteArray()).isEqualTo(remoteOhId);
    assertThat(forwarded.getContent().toByteArray()).isEqualTo(payload);
    assertThat(forwarded.getSessionTag().toByteArray())
        .as("the session tag must survive the MS02b fallback")
        .isEqualTo(sessionTag);
  }
}
