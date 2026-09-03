package im.redpanda.routing;

import im.redpanda.core.KademliaId;
import im.redpanda.core.ServerContext;
import im.redpanda.mailbox.OutboundService;
import im.redpanda.mailbox.ReturnPath;
import im.redpanda.mailbox.RoutingAckSender;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Objects;
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

  /**
   * Global rate limiter for hop-carrying reverse-garlic emission (L1, bug hunt 2026-07-26).
   *
   * <p>The return-path hop descriptors are chosen entirely by the sender and never validated
   * against the relays' real keys, so a deposit with a crafted return path makes this node emit an
   * onion packet at an address of the attacker's choosing — bounded reflection. Amplification is
   * already low (at most {@link ReturnPath#MAX_HOPS} hops, one ack-sized packet per deposit,
   * fire-and-forget), but nothing capped the *rate* of attacker-driven emission. A single global
   * bucket bounds it, following the same pattern as the record-store and record-lookup limiters in
   * {@link GarlicRouter}: an unattributable garlic-wrapped path gets a global cap, not a per-source
   * one.
   *
   * <p>Only the hop-carrying path is limited. With {@code hop_count = 0} this node is the final
   * station itself — a local mailbox deposit or a normal MS02b forward toward the ack OH host, no
   * sender-chosen destination and therefore no reflection — so the common direct-deposit R-ACK is
   * never dropped by this.
   */
  private static volatile RecordStoreRateLimiter reflectionRateLimiter =
      new RecordStoreRateLimiter(
          RecordStoreRateLimiter.DEFAULT_CAPACITY,
          RecordStoreRateLimiter.DEFAULT_REFILL_INTERVAL_MS,
          System.currentTimeMillis());

  private ReverseGarlic() {}

  /**
   * Test-only hook: swaps the reflection rate limiter for a small, deterministic bucket so tests
   * can exercise exhaustion without wall-clock timing. Returns the previous instance so the caller
   * can restore it.
   */
  static RecordStoreRateLimiter swapReflectionRateLimiterForTest(RecordStoreRateLimiter limiter) {
    RecordStoreRateLimiter previous = reflectionRateLimiter;
    reflectionRateLimiter = Objects.requireNonNull(limiter);
    return previous;
  }

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
    if (!reflectionRateLimiter.tryAcquire(System.currentTimeMillis())) {
      log.debug("reverse-garlic emission rate limit reached, dropping payload");
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
    returnPath.ackOhId().writeTo(deliver);
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
