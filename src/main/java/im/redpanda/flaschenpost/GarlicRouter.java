package im.redpanda.flaschenpost;

import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.core.Command;
import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.jobs.KademliaInsertJob;
import im.redpanda.jobs.RecordLookupJob;
import im.redpanda.kademlia.KadContent;
import im.redpanda.outbound.ChannelDht;
import im.redpanda.outbound.OutboundService;
import im.redpanda.proto.KademliaStore;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;

/**
 * MS04 stateless garlic relay: handles inbound {@link Command#FLASCHENPOST_V2} packets.
 *
 * <ul>
 *   <li>Dedup: a {@code packet_id} is processed at most once per node (5-minute window). This stops
 *       replays and routing loops of the <em>unchanged</em> packet (the Kademlia steps between
 *       garlic hops keep the packet_id). Peeled {@code CMD_FORWARD} chains rebuild with a fresh
 *       packet_id, but their length is physically bounded by the layer count: every layer costs 85
 *       bytes of the fixed 2048-byte packet, so a sender cannot construct unbounded relay loops.
 *       The packet intentionally carries no hop counter (it would leak the position in the path).
 *   <li>If {@code next_hop} is not us, the packet is routed unchanged toward {@code next_hop}
 *       (Kademlia step between garlic hops).
 *   <li>If it is us, the layer is peeled: {@code CMD_FORWARD} rebuilds a fresh 2048-byte packet
 *       (new random packet_id, new random padding) and routes it to the inner next hop; {@code
 *       CMD_DELIVER} deposits the payload into the local OH mailbox (falling back to the MS02b
 *       {@link OhForwarder} if the OH is hosted on another node).
 *   <li>Any malformed or unauthenticated packet is dropped silently (best-effort layer, no error
 *       responses).
 * </ul>
 */
@Slf4j
public final class GarlicRouter {

  private static final SecureRandom RANDOM = new SecureRandom();

  /**
   * Global rate limiter for inbound channel-rendezvous record stores (T43). Stores arrive
   * garlic-wrapped with no attributable source, so a single global cap bounds the DHT-write
   * amplification a flood can trigger on this node.
   */
  private static final RecordStoreRateLimiter RECORD_STORE_RATE_LIMITER =
      new RecordStoreRateLimiter(
          RecordStoreRateLimiter.DEFAULT_CAPACITY,
          RecordStoreRateLimiter.DEFAULT_REFILL_INTERVAL_MS,
          System.currentTimeMillis());

  private GarlicRouter() {}

  /** Entry point for a received FLASCHENPOST_V2 payload (the raw 2048-byte packet). */
  public static void handle(ServerContext serverContext, byte[] packet) {
    FlaschenpostV2 fp = FlaschenpostV2.parse(packet);
    if (fp == null) {
      log.debug("dropping malformed flaschenpost v2 packet");
      return;
    }
    if (GMStoreManager.putV2(fp.getPacketId())) {
      log.debug("dropping duplicate flaschenpost v2 packet {}", fp.getPacketId());
      return;
    }
    if (!fp.getNextHop().equals(serverContext.getNonce())) {
      routeToNextHop(serverContext, fp.getNextHop(), packet);
      return;
    }

    byte[] plaintext;
    try {
      plaintext = fp.decryptLayer(serverContext.getNodeId().getEncryptionKey());
    } catch (GeneralSecurityException e) {
      // not encrypted to us or tampered with — drop silently, never flood-forward
      log.debug("flaschenpost v2 layer authentication failed, dropping packet");
      return;
    }

    byte cmd = plaintext[0]; // MIN_CIPHERTEXT_LEN guarantees at least one plaintext byte
    switch (cmd) {
      case FlaschenpostV2.CMD_FORWARD -> handleForward(serverContext, plaintext);
      case FlaschenpostV2.CMD_DELIVER -> handleDeliver(serverContext, plaintext, false);
      case FlaschenpostV2.CMD_DELIVER_TAGGED -> handleDeliver(serverContext, plaintext, true);
      case FlaschenpostV2.CMD_DELIVER_ACKED -> handleDeliverAcked(serverContext, plaintext);
      case FlaschenpostV2.CMD_RECORD_STORE -> handleRecordStore(serverContext, plaintext);
      case FlaschenpostV2.CMD_RECORD_LOOKUP -> handleRecordLookup(serverContext, plaintext);
      default -> log.debug("unknown flaschenpost v2 layer command {}, dropping", cmd);
    }
  }

  /** CMD_FORWARD: [1 cmd][20 next_hop][body = nonce|ephemeral_pub|ct_len|ct]. */
  private static void handleForward(ServerContext serverContext, byte[] plaintext) {
    int minLen =
        1
            + KademliaId.ID_LENGTH_BYTES
            + FlaschenpostV2.BODY_HEADER_LEN
            + FlaschenpostV2.MIN_CIPHERTEXT_LEN;
    if (plaintext.length < minLen) {
      log.debug("flaschenpost v2 forward layer too short, dropping");
      return;
    }
    KademliaId innerNextHop =
        KademliaId.fromBuffer(ByteBuffer.wrap(plaintext, 1, KademliaId.ID_LENGTH_BYTES));
    byte[] body = Arrays.copyOfRange(plaintext, 1 + KademliaId.ID_LENGTH_BYTES, plaintext.length);

    byte[] rebuilt;
    try {
      // fresh random packet_id so the dedup caches on the next hops never collide
      rebuilt = FlaschenpostV2.buildPacket(RANDOM.nextInt(), innerNextHop, body);
    } catch (IllegalArgumentException e) {
      log.debug("flaschenpost v2 inner packet invalid, dropping: {}", e.getMessage());
      return;
    }
    if (FlaschenpostV2.parse(rebuilt) == null) {
      log.debug("rebuilt flaschenpost v2 packet invalid, dropping");
      return;
    }
    routeToNextHop(serverContext, innerNextHop, rebuilt);
  }

  /**
   * CMD_DELIVER: {@code [1 cmd][20 oh_id][4 payload_len][payload][ignored padding]}.
   *
   * <p>CMD_DELIVER_TAGGED (MS05, {@code tagged = true}): {@code [1 cmd][20 oh_id][16 session_tag][4
   * payload_len][payload][ignored padding]} — the session tag is deposited with the payload so the
   * fetching client can correlate the reverse-garlic reply with a conversation.
   */
  private static void handleDeliver(ServerContext serverContext, byte[] plaintext, boolean tagged) {
    int tagLen = tagged ? FlaschenpostV2.SESSION_TAG_LEN : 0;
    if (plaintext.length < 1 + KademliaId.ID_LENGTH_BYTES + tagLen + 4) {
      log.debug("flaschenpost v2 deliver layer too short, dropping");
      return;
    }
    ByteBuffer buffer = ByteBuffer.wrap(plaintext);
    buffer.get(); // command byte
    byte[] ohId = new byte[KademliaId.ID_LENGTH_BYTES];
    buffer.get(ohId);
    byte[] sessionTag = new byte[tagLen];
    buffer.get(sessionTag);
    int payloadLen = buffer.getInt();
    if (payloadLen < 0 || payloadLen > buffer.remaining()) {
      log.debug("flaschenpost v2 deliver payload length invalid, dropping");
      return;
    }
    byte[] payload = new byte[payloadLen];
    buffer.get(payload);

    OutboundService outboundService = serverContext.getOutboundService();
    if (outboundService == null) {
      return;
    }
    OutboundService.DepositResult result =
        outboundService.depositMessage(ohId, payload, sessionTag);
    if (result == OutboundService.DepositResult.NOT_FOUND) {
      // the final garlic hop does not have to be the OH host — reuse the MS02b forwarding
      // (the session tag rides along on the FlaschenpostPut, see OhForwarder)
      OhForwarder.forward(serverContext, ohId, payload, 0, sessionTag);
    } else if (result != OutboundService.DepositResult.DEPOSITED) {
      log.debug("flaschenpost v2 deliver not stored: {}", result);
    }
  }

  /**
   * CMD_DELIVER_ACKED (MS06): {@code [1 cmd][20 oh_id][1 tag_len (0|16)][tag_len session_tag]
   * [return_path][4 payload_len][payload][ignored padding]} — a deliver that requests an R-ACK.
   *
   * <p>The deposit semantics match {@link #handleDeliver}; additionally the node that makes the
   * final deposit decision sends a {@code RoutingAck} back through the return path. If the OH is
   * hosted elsewhere, the return path rides along on the MS02b {@code FlaschenpostPut} (like the
   * MS05 session tag) and the host node acks. Malformed layers are dropped silently — the return
   * path is only structurally validated (lengths, hop bounds), its contents are the sender's
   * responsibility (an unreachable ack path just means no R-ACK arrives).
   */
  private static void handleDeliverAcked(ServerContext serverContext, byte[] plaintext) {
    // minimum: cmd + oh_id + tag_len byte + empty tag + fixed return path + payload_len
    if (plaintext.length < 1 + KademliaId.ID_LENGTH_BYTES + 1 + ReturnPath.FIXED_LEN + 4) {
      log.debug("flaschenpost v2 acked deliver layer too short, dropping");
      return;
    }
    ByteBuffer buffer = ByteBuffer.wrap(plaintext);
    buffer.get(); // command byte
    byte[] ohId = new byte[KademliaId.ID_LENGTH_BYTES];
    buffer.get(ohId);
    int tagLen = buffer.get() & 0xFF;
    if (tagLen != 0 && tagLen != FlaschenpostV2.SESSION_TAG_LEN) {
      log.debug("flaschenpost v2 acked deliver tag length invalid, dropping");
      return;
    }
    if (buffer.remaining() < tagLen) {
      log.debug("flaschenpost v2 acked deliver layer too short, dropping");
      return;
    }
    byte[] sessionTag = new byte[tagLen];
    buffer.get(sessionTag);
    ReturnPath returnPath = ReturnPath.parse(buffer);
    if (returnPath == null || buffer.remaining() < 4) {
      log.debug("flaschenpost v2 acked deliver return path invalid, dropping");
      return;
    }
    int payloadLen = buffer.getInt();
    if (payloadLen < 0 || payloadLen > buffer.remaining()) {
      log.debug("flaschenpost v2 acked deliver payload length invalid, dropping");
      return;
    }
    byte[] payload = new byte[payloadLen];
    buffer.get(payload);

    OutboundService outboundService = serverContext.getOutboundService();
    if (outboundService == null) {
      return;
    }
    OutboundService.DepositResult result =
        outboundService.depositMessage(ohId, payload, sessionTag);
    if (result == OutboundService.DepositResult.NOT_FOUND) {
      // not our OH — the MS02b forward conserves the return path so the host node acks.
      // The hop budget starts at 0 here, so the forward is always accepted; the hop-limit
      // handle_expired R-ACK is handled on the FlaschenpostPut path (InboundCommandProcessor).
      OhForwarder.forward(serverContext, ohId, payload, 0, sessionTag, returnPath.serialize());
      return;
    }
    RoutingAckSender.send(serverContext, returnPath, RoutingAckSender.statusFor(result));
  }

  /**
   * CMD_RECORD_STORE (T43): {@code [1 cmd][4 len][KademliaStore proto][ignored padding]}. Stores a
   * channel-rendezvous record in the DHT on behalf of a DHT-fremd client. The record content is
   * opaque to us; we only enforce the self-certifying signature, the fixed padded size and the 48 h
   * TTL ({@link ChannelDht}) plus a global store rate limit. Best-effort, no response — the client
   * verifies by a later lookup. Malformed layers are dropped silently.
   */
  private static void handleRecordStore(ServerContext serverContext, byte[] plaintext) {
    if (plaintext.length < 1 + 4) {
      log.debug("flaschenpost v2 record store layer too short, dropping");
      return;
    }
    ByteBuffer buffer = ByteBuffer.wrap(plaintext);
    buffer.get(); // command byte
    int len = buffer.getInt();
    if (len < 0 || len > buffer.remaining()) {
      log.debug("flaschenpost v2 record store length invalid, dropping");
      return;
    }
    byte[] storeBytes = new byte[len];
    buffer.get(storeBytes);

    KademliaStore storeMsg;
    try {
      storeMsg = KademliaStore.parseFrom(storeBytes);
    } catch (InvalidProtocolBufferException e) {
      log.debug("flaschenpost v2 record store payload not parseable, dropping");
      return;
    }
    long now = System.currentTimeMillis();
    KadContent record =
        new KadContent(
            storeMsg.getTimestamp(),
            storeMsg.getPublicKey().toByteArray(),
            storeMsg.getContent().toByteArray(),
            storeMsg.getSignature().toByteArray());
    if (!ChannelDht.isValidRecord(record, now)) {
      log.debug("flaschenpost v2 record store record invalid (sig/size/ttl), dropping");
      return;
    }
    if (!RECORD_STORE_RATE_LIMITER.tryAcquire(now)) {
      log.debug("channel record store rate limit hit, dropping");
      return;
    }
    // Store locally first so the record is immediately resolvable on this node. Only replicate if
    // the local put was accepted — KadStoreManager can still reject on its own timestamp bounds,
    // and
    // replicating a record we ourselves won't keep would be pointless.
    if (serverContext.getKadStoreManager().put(record)) {
      new KademliaInsertJob(serverContext, record).start();
    }
  }

  /**
   * CMD_RECORD_LOOKUP (T43): {@code [1 cmd][20 kademlia_key][return_path][ignored padding]}. Looks
   * the channel-rendezvous record up in the DHT on behalf of a DHT-fremd client and returns it via
   * the return path (reverse garlic into the client's own OH mailbox, see {@link RecordLookupJob}).
   * The return path is only structurally validated; malformed layers are dropped silently.
   */
  private static void handleRecordLookup(ServerContext serverContext, byte[] plaintext) {
    if (plaintext.length < 1 + KademliaId.ID_LENGTH_BYTES + ReturnPath.FIXED_LEN) {
      log.debug("flaschenpost v2 record lookup layer too short, dropping");
      return;
    }
    ByteBuffer buffer = ByteBuffer.wrap(plaintext);
    buffer.get(); // command byte
    KademliaId searchedKey = KademliaId.fromBuffer(buffer);
    ReturnPath returnPath = ReturnPath.parse(buffer);
    if (returnPath == null) {
      log.debug("flaschenpost v2 record lookup return path invalid, dropping");
      return;
    }
    RecordLookupJob.lookup(serverContext, searchedKey, returnPath);
  }

  /**
   * Routes a (raw, still 2048-byte) packet toward {@code nextHop} using the shared next-peer
   * selection: direct connection, weighted graph route or greedy Kademlia step. Best-effort —
   * without a route the packet is dropped silently.
   */
  static void routeToNextHop(ServerContext serverContext, KademliaId nextHop, byte[] packet) {
    Peer peer = OhForwarder.selectNextPeer(serverContext, nextHop);
    if (peer == null) {
      log.debug("no route toward flaschenpost v2 next hop {}, dropping", nextHop);
      return;
    }
    sendToPeer(peer, packet);
  }

  /** Writes a FLASCHENPOST_V2 frame ([cmd][len:4][packet]) to the peer. */
  public static void sendToPeer(Peer peer, byte[] packet) {
    peer.getWriteBufferLock().lock();
    try {
      peer.writeBuffer.put(Command.FLASCHENPOST_V2);
      peer.writeBuffer.putInt(packet.length);
      peer.writeBuffer.put(packet);
      peer.setWriteBufferFilled();
    } finally {
      peer.getWriteBufferLock().unlock();
    }
  }
}
