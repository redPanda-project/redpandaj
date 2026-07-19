package im.redpanda.flaschenpost;

import im.redpanda.core.ServerContext;
import im.redpanda.outbound.OutboundService;
import im.redpanda.outbound.v1.RoutingAck;

/**
 * MS06 R-ACK dispatch: builds the {@link RoutingAck} for a deposit decision and sends it back
 * through the sender-chosen {@link ReturnPath} via {@link ReverseGarlic} (a standard MS04 onion
 * whose innermost {@code CMD_DELIVER_TAGGED} layer lands in the sender's own OH mailbox, tagged
 * with the ack session tag).
 *
 * <p>Best-effort and fire-and-forget like the rest of the flaschenpost layer: build or routing
 * failures are logged and dropped, there are no retries (the sender's R-ACK timeout plus MS02-style
 * re-sends cover losses). R-ACKs themselves never request an R-ACK ({@code CMD_DELIVER_TAGGED}
 * innermost layer), so acknowledgment loops are impossible by construction. Amplification is
 * bounded at factor one: a 2048-byte acked deliver triggers at most one 2048-byte R-ACK packet.
 */
public final class RoutingAckSender {

  /** RoutingAck.status: message stored in the target OH mailbox. */
  public static final int STATUS_STORED = 0;

  /** RoutingAck.status: target mailbox full (item cap or byte quota), nothing displaced. */
  public static final int STATUS_MAILBOX_FULL = 1;

  /** RoutingAck.status: target OH unknown or expired at the final station (no forward budget). */
  public static final int STATUS_HANDLE_EXPIRED = 2;

  /** RoutingAck.status: deposit rejected as bad request (e.g. oversized payload). */
  public static final int STATUS_REJECTED = 3;

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
   * Builds the RoutingAck payload and sends it along the return path via {@link ReverseGarlic}.
   * With hops the packet is a standard MS04 onion routed toward the first hop; without hops ({@code
   * hop_count = 0}) the depositing node acts as the final station itself (local deposit into the
   * ack OH, MS02b-forwarding if remote). The forwarded deposit carries no return path — R-ACKs are
   * never acknowledged.
   */
  public static void send(ServerContext serverContext, ReturnPath returnPath, int status) {
    byte[] rAck =
        RoutingAck.newBuilder()
            .setTimestampMs(System.currentTimeMillis())
            .setStatus(status)
            .build()
            .toByteArray();
    ReverseGarlic.sendTaggedPayload(serverContext, returnPath, rAck);
  }
}
