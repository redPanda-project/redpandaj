package im.redpanda.flaschenpost;

import im.redpanda.core.KademliaId;
import im.redpanda.core.ServerContext;
import im.redpanda.outbound.OutboundService;
import im.redpanda.outbound.v1.RoutingAck;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;

/**
 * MS06 R-ACK dispatch: builds the {@link RoutingAck} for a deposit decision and sends it back
 * through the sender-chosen {@link ReturnPath} as a standard MS04 onion (innermost layer = {@code
 * CMD_DELIVER_TAGGED} into the sender's own OH mailbox, tagged with the ack session tag).
 *
 * <p>Best-effort and fire-and-forget like the rest of the flaschenpost layer: build or routing
 * failures are logged and dropped, there are no retries (the sender's R-ACK timeout plus MS02-style
 * re-sends cover losses). R-ACKs themselves never request an R-ACK ({@code CMD_DELIVER_TAGGED}
 * innermost layer), so acknowledgment loops are impossible by construction. Amplification is
 * bounded at factor one: a 2048-byte acked deliver triggers at most one 2048-byte R-ACK packet.
 */
@Slf4j
public final class RoutingAckSender {

  /** RoutingAck.status: message stored in the target OH mailbox. */
  public static final int STATUS_STORED = 0;

  /** RoutingAck.status: target mailbox full (item cap or byte quota), nothing displaced. */
  public static final int STATUS_MAILBOX_FULL = 1;

  /** RoutingAck.status: target OH unknown or expired at the final station (no forward budget). */
  public static final int STATUS_HANDLE_EXPIRED = 2;

  /** RoutingAck.status: deposit rejected as bad request (e.g. oversized payload). */
  public static final int STATUS_REJECTED = 3;

  private static final SecureRandom RANDOM = new SecureRandom();

  private RoutingAckSender() {}

  /**
   * Maps the final deposit decision to the R-ACK status. {@code NOT_FOUND} maps to {@code
   * handle_expired} — callers only ask for a status once forwarding is no longer possible, so "not
   * found here" is final at that point.
   */
  public static int statusFor(OutboundService.DepositResult result) {
    return switch (result) {
      case DEPOSITED -> STATUS_STORED;
      case QUOTA_EXCEEDED -> STATUS_MAILBOX_FULL;
      case NOT_FOUND -> STATUS_HANDLE_EXPIRED;
      case BAD_REQUEST -> STATUS_REJECTED;
    };
  }

  /**
   * Builds the RoutingAck payload and sends it along the return path. With hops the packet is a
   * standard MS04 onion routed toward the first hop; without hops ({@code hop_count = 0}) this node
   * acts as the final station itself: local deposit into the ack OH, falling back to the MS02b
   * forward toward its host node. The forwarded deposit carries no return path — R-ACKs are never
   * acknowledged.
   */
  public static void send(ServerContext serverContext, ReturnPath returnPath, int status) {
    byte[] rAck =
        RoutingAck.newBuilder()
            .setTimestampMs(System.currentTimeMillis())
            .setStatus(status)
            .build()
            .toByteArray();

    List<ReturnPath.Hop> hops = returnPath.hops();
    if (hops.isEmpty()) {
      deliverLocally(serverContext, returnPath, rAck);
      return;
    }

    byte[] packet;
    try {
      packet = buildAckOnion(returnPath, rAck);
    } catch (GeneralSecurityException | IllegalArgumentException e) {
      log.debug("failed to build R-ACK onion, dropping: {}", e.getMessage());
      return;
    }
    GarlicRouter.routeToNextHop(serverContext, hops.get(0).kademliaId(), packet);
  }

  /** Deposits the R-ACK on this node (hop_count = 0), MS02b-forwarding if the OH is remote. */
  private static void deliverLocally(
      ServerContext serverContext, ReturnPath returnPath, byte[] rAck) {
    OutboundService outboundService = serverContext.getOutboundService();
    if (outboundService == null) {
      return;
    }
    OutboundService.DepositResult result =
        outboundService.depositMessage(returnPath.ackOhId(), rAck, returnPath.ackSessionTag());
    if (result == OutboundService.DepositResult.NOT_FOUND) {
      OhForwarder.forward(serverContext, returnPath.ackOhId(), rAck, 0, returnPath.ackSessionTag());
    } else if (result != OutboundService.DepositResult.DEPOSITED) {
      log.debug("R-ACK not stored locally: {}", result);
    }
  }

  /**
   * Builds the R-ACK packet exactly like a client-side MS04 send: innermost {@code
   * CMD_DELIVER_TAGGED} layer for the last hop, wrapped in {@code CMD_FORWARD} layers along the
   * return path in reverse order.
   */
  private static byte[] buildAckOnion(ReturnPath returnPath, byte[] rAck)
      throws GeneralSecurityException {
    List<ReturnPath.Hop> hops = returnPath.hops();
    ReturnPath.Hop last = hops.get(hops.size() - 1);

    ByteBuffer deliver =
        ByteBuffer.allocate(
            1 + KademliaId.ID_LENGTH_BYTES + FlaschenpostV2.SESSION_TAG_LEN + 4 + rAck.length);
    deliver.put(FlaschenpostV2.CMD_DELIVER_TAGGED);
    deliver.put(returnPath.ackOhId());
    deliver.put(returnPath.ackSessionTag());
    deliver.putInt(rAck.length);
    deliver.put(rAck);

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
