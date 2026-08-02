package im.redpanda.flaschenpost;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.Command;
import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.outbound.OutboundHandleStore;
import im.redpanda.outbound.OutboundMailboxStore;
import im.redpanda.outbound.OutboundService;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for L1 (bug hunt 2026-07-26): the return-path hop descriptors are chosen
 * entirely by the sender and never validated, so a deposit with a crafted return path makes this
 * node emit an onion packet at an address of the attacker's choosing. Amplification was already
 * bounded (≤ {@link ReturnPath#MAX_HOPS} hops, one ack-sized packet, fire-and-forget), but nothing
 * capped the rate of attacker-driven emission.
 */
class ReverseGarlicReflectionLimitTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  private ServerContext relay;
  private ServerContext target;
  private OutboundHandleStore handleStore;
  private OutboundMailboxStore mailboxStore;
  private Peer relayToTarget;
  private RecordStoreRateLimiter previousLimiter;

  @BeforeEach
  void setUp() {
    relay = ServerContext.buildDefaultServerContext();
    handleStore = new OutboundHandleStore();
    mailboxStore = new OutboundMailboxStore();
    relay.setOutboundService(new OutboundService(handleStore, mailboxStore));

    target = ServerContext.buildDefaultServerContext();

    relayToTarget = new Peer("127.0.0.1", 9601, target.getNodeId());
    relayToTarget.setConnected(true);
    relayToTarget.writeBuffer = ByteBuffer.allocate(65536);
    relay.getPeerList().add(relayToTarget);
  }

  @AfterEach
  void tearDown() {
    if (previousLimiter != null) {
      ReverseGarlic.swapReflectionRateLimiterForTest(previousLimiter);
    }
  }

  @Test
  void hopCarryingEmissionIsRateLimited() {
    // one token, refilled once a minute → the second emission is deterministically over budget
    previousLimiter =
        ReverseGarlic.swapReflectionRateLimiterForTest(
            new RecordStoreRateLimiter(1, 60_000L, System.currentTimeMillis()));

    ReturnPath attackerChosenPath = pathVia(target);

    ReverseGarlic.sendTaggedPayload(relay, attackerChosenPath, payload());
    assertThat(emittedFrames()).as("the first hop-carrying emission is admitted").isEqualTo(1);

    ReverseGarlic.sendTaggedPayload(relay, attackerChosenPath, payload());
    ReverseGarlic.sendTaggedPayload(relay, attackerChosenPath, payload());
    assertThat(emittedFrames())
        .as("further emissions must be dropped while the bucket is empty")
        .isZero();
  }

  /**
   * hop_count = 0 is not a reflection vector — this node is the final station itself — so it must
   * keep working even with an exhausted bucket.
   */
  @Test
  void zeroHopDeliveryIsNotRateLimited() {
    previousLimiter =
        ReverseGarlic.swapReflectionRateLimiterForTest(
            new RecordStoreRateLimiter(1, 60_000L, System.currentTimeMillis()));

    byte[] ohId = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(ohId);
    long now = System.currentTimeMillis();
    handleStore.put(ohId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));

    ReturnPath direct = new ReturnPath(ohId, sessionTag(), List.of());
    for (int i = 0; i < 5; i++) {
      ReverseGarlic.sendTaggedPayload(relay, direct, payload());
    }

    assertThat(mailboxStore.fetchMessages(ohId, 10, 0))
        .as("direct deposits must never be dropped by the reflection limiter")
        .hasSize(5);
  }

  /** A return path whose only hop is {@code hop} — the "reflect at this node" shape. */
  private ReturnPath pathVia(ServerContext hop) {
    byte[] ohId = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(ohId);
    return new ReturnPath(
        ohId,
        sessionTag(),
        List.of(
            new ReturnPath.Hop(
                hop.getNonce(), hop.getNodeId().getEncryptionPubKey().getEncoded())));
  }

  private static byte[] sessionTag() {
    byte[] tag = new byte[FlaschenpostV2.SESSION_TAG_LEN];
    RANDOM.nextBytes(tag);
    return tag;
  }

  private static byte[] payload() {
    return "r-ack".getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  /** Counts (and drains) the FLASCHENPOST_V2 frames this relay wrote toward the target. */
  private int emittedFrames() {
    ByteBuffer out = relayToTarget.writeBuffer;
    out.flip();
    int frames = 0;
    while (out.hasRemaining()) {
      assertThat(out.get()).isEqualTo(Command.FLASCHENPOST_V2);
      byte[] packet = new byte[out.getInt()];
      out.get(packet);
      frames++;
    }
    out.clear();
    return frames;
  }
}
