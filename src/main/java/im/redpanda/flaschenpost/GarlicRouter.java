package im.redpanda.flaschenpost;

import im.redpanda.core.Command;
import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.outbound.OutboundService;
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
      case FlaschenpostV2.CMD_DELIVER -> handleDeliver(serverContext, plaintext);
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

  /** CMD_DELIVER: [1 cmd][20 oh_id][4 payload_len][payload][ignored padding]. */
  private static void handleDeliver(ServerContext serverContext, byte[] plaintext) {
    if (plaintext.length < 1 + KademliaId.ID_LENGTH_BYTES + 4) {
      log.debug("flaschenpost v2 deliver layer too short, dropping");
      return;
    }
    ByteBuffer buffer = ByteBuffer.wrap(plaintext);
    buffer.get(); // command byte
    byte[] ohId = new byte[KademliaId.ID_LENGTH_BYTES];
    buffer.get(ohId);
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
    OutboundService.DepositResult result = outboundService.depositMessage(ohId, payload);
    if (result == OutboundService.DepositResult.NOT_FOUND) {
      // the final garlic hop does not have to be the OH host — reuse the MS02b forwarding
      OhForwarder.forward(serverContext, ohId, payload, 0);
    } else if (result != OutboundService.DepositResult.DEPOSITED) {
      log.debug("flaschenpost v2 deliver not stored: {}", result);
    }
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
