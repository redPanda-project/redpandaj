package im.redpanda.flaschenpost;

import im.redpanda.core.KademliaId;
import im.redpanda.core.ServerContext;
import im.redpanda.outbound.OutboundService;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;

/**
 * Shared reverse-garlic dispatch: sends an arbitrary payload back to a sender through the
 * sender-chosen {@link ReturnPath} as a standard MS04 onion whose innermost {@code
 * CMD_DELIVER_TAGGED} layer lands in the sender's own OH mailbox under the ack session tag.
 *
 * <p>This is the mechanism MS06 R-ACKs use ({@link RoutingAckSender}) and that T43 reuses to return
 * a channel-rendezvous record to a DHT-fremd light client (the answer to a {@code record_lookup}).
 * Best-effort and fire-and-forget like the rest of the flaschenpost layer: build or routing
 * failures are logged and dropped, there are no retries. With {@code hop_count = 0} this node acts
 * as the final station itself (local deposit into the ack OH, MS02b-forwarding if the OH is
 * remote).
 */
@Slf4j
public final class ReverseGarlic {

  private static final SecureRandom RANDOM = new SecureRandom();

  private ReverseGarlic() {}

  /**
   * Sends {@code payload} back along {@code returnPath}. The payload is deposited into the ack OH
   * as a tagged MailItem; the sender correlates it with its request via the ack session tag it
   * chose.
   */
  public static void sendTaggedPayload(
      ServerContext serverContext, ReturnPath returnPath, byte[] payload) {
    List<ReturnPath.Hop> hops = returnPath.hops();
    if (hops.isEmpty()) {
      deliverLocally(serverContext, returnPath, payload);
      return;
    }
    byte[] packet;
    try {
      packet = buildOnion(returnPath, payload);
    } catch (GeneralSecurityException | IllegalArgumentException e) {
      log.debug("failed to build reverse-garlic onion, dropping: {}", e.getMessage());
      return;
    }
    GarlicRouter.routeToNextHop(serverContext, hops.get(0).kademliaId(), packet);
  }

  /** Deposits the payload on this node (hop_count = 0), MS02b-forwarding if the OH is remote. */
  private static void deliverLocally(
      ServerContext serverContext, ReturnPath returnPath, byte[] payload) {
    OutboundService outboundService = serverContext.getOutboundService();
    if (outboundService == null) {
      return;
    }
    OutboundService.DepositResult result =
        outboundService.depositMessage(returnPath.ackOhId(), payload, returnPath.ackSessionTag());
    if (result == OutboundService.DepositResult.NOT_FOUND) {
      OhForwarder.forward(
          serverContext, returnPath.ackOhId(), payload, 0, returnPath.ackSessionTag());
    } else if (result != OutboundService.DepositResult.DEPOSITED) {
      log.debug("reverse-garlic payload not stored locally: {}", result);
    }
  }

  /**
   * Builds the packet exactly like a client-side MS04 send: innermost {@code CMD_DELIVER_TAGGED}
   * layer for the last hop, wrapped in {@code CMD_FORWARD} layers along the return path in reverse
   * order.
   */
  private static byte[] buildOnion(ReturnPath returnPath, byte[] payload)
      throws GeneralSecurityException {
    List<ReturnPath.Hop> hops = returnPath.hops();
    ReturnPath.Hop last = hops.get(hops.size() - 1);

    ByteBuffer deliver =
        ByteBuffer.allocate(
            1 + KademliaId.ID_LENGTH_BYTES + FlaschenpostV2.SESSION_TAG_LEN + 4 + payload.length);
    deliver.put(FlaschenpostV2.CMD_DELIVER_TAGGED);
    deliver.put(returnPath.ackOhId());
    deliver.put(returnPath.ackSessionTag());
    deliver.putInt(payload.length);
    deliver.put(payload);

    byte[] body =
        FlaschenpostV2.encryptLayer(
            new X25519PublicKeyParameters(last.encryptionPub(), 0),
            last.kademliaId(),
            deliver.array());

    for (int i = hops.size() - 2; i >= 0; i--) {
      ReturnPath.Hop hop = hops.get(i);
      ByteBuffer forward = ByteBuffer.allocate(1 + KademliaId.ID_LENGTH_BYTES + body.length);
      forward.put(FlaschenpostV2.CMD_FORWARD);
      forward.put(hops.get(i + 1).kademliaId().getBytes());
      forward.put(body);
      body =
          FlaschenpostV2.encryptLayer(
              new X25519PublicKeyParameters(hop.encryptionPub(), 0),
              hop.kademliaId(),
              forward.array());
    }
    return FlaschenpostV2.buildPacket(RANDOM.nextInt(), hops.get(0).kademliaId(), body);
  }
}
