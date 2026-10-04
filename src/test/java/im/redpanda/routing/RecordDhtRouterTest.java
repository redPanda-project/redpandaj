package im.redpanda.routing;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import im.redpanda.core.ServerContext;
import im.redpanda.dht.ChannelDht;
import im.redpanda.dht.KadContent;
import im.redpanda.dht.RecordLookupJob;
import im.redpanda.identity.KademliaId;
import im.redpanda.mailbox.OhId;
import im.redpanda.mailbox.OutboundHandleStore;
import im.redpanda.mailbox.OutboundMailboxStore;
import im.redpanda.mailbox.OutboundService;
import im.redpanda.mailbox.OutboundStore;
import im.redpanda.mailbox.ReturnPath;
import im.redpanda.outbound.v1.MailItem;
import im.redpanda.proto.KademliaStore;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * T43 acceptance: a garlic-wrapped {@code CMD_RECORD_STORE} stores a channel-rendezvous record in
 * the node's DHT, and a garlic-wrapped {@code CMD_RECORD_LOOKUP} resolves it and returns it via the
 * client-chosen return path (reverse garlic into the client's own OH mailbox). Uses a hop_count = 0
 * return path so the answer is deposited locally and deterministically, mirroring the MS06 R-ACK
 * router test. Also covers the not-found answer and that an invalid record is not stored.
 */
class RecordDhtRouterTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  /**
   * Explicit packet IDs for the rate-limit test (TD001): random per call, but never below this
   * offset, so they can neither hit the small literals other tests use nor repeat on a rerun of the
   * same test in the same fork (the GMStoreManager dedup would keep them for 5 minutes).
   */
  private static final int LARGE_PACKET_ID_OFFSET = 0x4000_0000;

  private static int largePacketId() {
    return LARGE_PACKET_ID_OFFSET + RANDOM.nextInt(Integer.MAX_VALUE - LARGE_PACKET_ID_OFFSET);
  }

  private ServerContext node;
  private OutboundMailboxStore mailbox;
  private OutboundHandleStore handles;
  private OhId ackOhId;
  private byte[] ackSessionTag;

  @BeforeEach
  void setUp() {
    node = ServerContext.buildDefaultServerContext();
    OutboundStore outboundStore = OutboundStore.inMemory();
    handles = outboundStore.handles();
    mailbox = outboundStore.mailbox();
    node.setOutboundService(new OutboundService(outboundStore));

    ackOhId = OhId.fromBytes(randomBytes(KademliaId.ID_LENGTH_BYTES));
    ackSessionTag = randomBytes(FlaschenpostV2.SESSION_TAG_LEN);
    long now = System.currentTimeMillis();
    handles.put(ackOhId, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));
  }

  private static byte[] randomBytes(int len) {
    byte[] bytes = new byte[len];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  private static byte[] randomChannelSecret() {
    return randomBytes(32);
  }

  /** hop_count = 0 return path — the lookup answer is deposited into the local ackOh. */
  private ReturnPath zeroHopReturnPath() {
    return new ReturnPath(ackOhId, ackSessionTag, List.of());
  }

  private byte[] recordStoreLayer(KadContent record) {
    byte[] store =
        KademliaStore.newBuilder()
            .setTimestamp(record.getTimestamp())
            .setPublicKey(ByteString.copyFrom(record.getPubkey()))
            .setContent(ByteString.copyFrom(record.getContent()))
            .setSignature(ByteString.copyFrom(record.getSignature()))
            .build()
            .toByteArray();
    return ByteBuffer.allocate(1 + 4 + store.length)
        .put(FlaschenpostV2.CMD_RECORD_STORE)
        .putInt(store.length)
        .put(store)
        .array();
  }

  private byte[] recordLookupLayer(KademliaId key, ReturnPath returnPath) {
    byte[] rp = returnPath.serialize();
    return ByteBuffer.allocate(1 + KademliaId.ID_LENGTH_BYTES + rp.length)
        .put(FlaschenpostV2.CMD_RECORD_LOOKUP)
        .put(key.getBytes())
        .put(rp)
        .array();
  }

  /** Encrypts a single-layer garlic packet carrying {@code plaintext} for our node. */
  private byte[] singleLayerPacket(byte[] plaintext) throws Exception {
    return singleLayerPacket(plaintext, RANDOM.nextInt());
  }

  /** Same as {@link #singleLayerPacket(byte[])} but with an explicit {@code packet_id}. */
  private byte[] singleLayerPacket(byte[] plaintext, int packetId) throws Exception {
    byte[] body =
        FlaschenpostV2.encryptLayer(
            node.getNodeId().getEncryptionPubKey(), node.getOwnNodeId(), plaintext);
    return FlaschenpostV2.buildPacket(packetId, node.getOwnNodeId(), body);
  }

  @Test
  void storeThenLookup_roundTripsRecordBackThroughReturnPath() throws Exception {
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();
    byte[] content = randomBytes(ChannelDht.RECORD_SIZE_BYTES);
    KadContent record = ChannelDht.buildRecordContent(secret, content, now);
    KademliaId key = ChannelDht.rendezvousKademliaId(secret, now);

    // 1) store
    GarlicRouter.handle(node, singleLayerPacket(recordStoreLayer(record)));
    KadContent stored = node.getKadStoreManager().get(key);
    assertThat(stored).as("record must be stored under the rendezvous key").isNotNull();
    assertThat(stored.getContent()).isEqualTo(record.getContent());

    // 2) lookup with a zero-hop return path → answer deposited into the local ackOh
    GarlicRouter.handle(node, singleLayerPacket(recordLookupLayer(key, zeroHopReturnPath())));

    List<MailItem> items = mailbox.fetchMessages(ackOhId, 10, 0);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).getSessionTag().toByteArray()).isEqualTo(ackSessionTag);

    byte[] answer = items.get(0).getPayload().toByteArray();
    assertThat(answer[0]).as("lookup found the record").isEqualTo(RecordLookupJob.RESPONSE_FOUND);
    KademliaStore returned =
        KademliaStore.parseFrom(java.util.Arrays.copyOfRange(answer, 1, answer.length));
    assertThat(returned.getContent().toByteArray())
        .as("the exact stored record content is returned")
        .isEqualTo(record.getContent());
    assertThat(returned.getContent().toByteArray())
        .as("returned opaque content matches what was stored")
        .isEqualTo(content);
  }

  @Test
  void lookup_unknownKey_returnsNotFound() throws Exception {
    KademliaId unknown =
        ChannelDht.rendezvousKademliaId(randomChannelSecret(), System.currentTimeMillis());

    // No local record and no peers → the node falls back to a (randomly delayed) DHT search that
    // finds nothing and still answers, so the client never waits out a timeout. Poll for the
    // asynchronous not-found answer.
    GarlicRouter.handle(node, singleLayerPacket(recordLookupLayer(unknown, zeroHopReturnPath())));

    List<MailItem> items = awaitMailbox(ackOhId);
    assertThat(items).hasSize(1);
    byte[] answer = items.get(0).getPayload().toByteArray();
    assertThat(answer).containsExactly(RecordLookupJob.RESPONSE_NOT_FOUND);
  }

  /**
   * Polls the mailbox until an item arrives (the not-found answer is dispatched asynchronously).
   */
  private List<MailItem> awaitMailbox(OhId ohId) throws InterruptedException {
    for (int i = 0; i < 120; i++) {
      List<MailItem> items = mailbox.fetchMessages(ohId, 10, 0);
      if (!items.isEmpty()) {
        return items;
      }
      Thread.sleep(50);
    }
    return mailbox.fetchMessages(ohId, 10, 0);
  }

  @Test
  void lookup_rateLimitExhausted_dropsSecondLookup() throws Exception {
    // Swap in a 1-token bucket with a refill interval far longer than any test run, so the second
    // call is deterministically over budget.
    RecordStoreRateLimiter previous =
        GarlicRouter.swapRecordLookupRateLimiterForTest(
            new RecordStoreRateLimiter(1, 60_000L, System.currentTimeMillis()));
    try {
      // TD147: the key resolves from the local KadStore, so an admitted lookup answers
      // synchronously inside GarlicRouter.handle (RecordLookupJob.lookup: local hit → respond, no
      // jittered DHT search). The rate limiter gates before that branch, so the mailbox count right
      // after each handle() call is final — no polling, no settle sleep. An unknown key would go
      // through the up-to-1.5 s randomized search job instead and need a fixed settle to rule out a
      // late 2nd answer, which is what made this test fail under CI load.
      byte[] secret = randomChannelSecret();
      long now = System.currentTimeMillis();
      KadContent record =
          ChannelDht.buildRecordContent(secret, randomBytes(ChannelDht.RECORD_SIZE_BYTES), now);
      KademliaId key = ChannelDht.rendezvousKademliaId(secret, now);
      assertThat(node.getKadStoreManager().put(record)).isTrue();
      byte[] layer = recordLookupLayer(key, zeroHopReturnPath());

      // TD001: large random packet IDs instead of small literals — the GMStoreManager packet_id
      // dedup is static (fork-global) for 5 minutes, so a literal reused by another test in the
      // fork would drop a packet as a duplicate instead of the rate limit doing it.
      int packetId = largePacketId();
      GarlicRouter.handle(node, singleLayerPacket(layer, packetId));
      assertThat(mailbox.fetchMessages(ackOhId, 10, 0))
          .as("the 1st lookup consumes the single token and is answered synchronously")
          .hasSize(1);

      GarlicRouter.handle(node, singleLayerPacket(layer, packetId + 1));
      assertThat(mailbox.fetchMessages(ackOhId, 10, 0))
          .as("the 2nd lookup finds the bucket empty and must be dropped without an answer")
          .hasSize(1);

      // Control: the identical lookup is answered again once a token is available, so the drop
      // above came from the rate limiter and not from anything else on the path (dedup, the
      // mailbox, the return path).
      GarlicRouter.swapRecordLookupRateLimiterForTest(
          new RecordStoreRateLimiter(1, 60_000L, System.currentTimeMillis()));
      GarlicRouter.handle(node, singleLayerPacket(layer, packetId + 2));
      assertThat(mailbox.fetchMessages(ackOhId, 10, 0))
          .as("with a fresh token the same lookup is answered again")
          .hasSize(2);
    } finally {
      GarlicRouter.swapRecordLookupRateLimiterForTest(previous);
    }
  }

  @Test
  void store_invalidRecord_isNotStored() throws Exception {
    // A record whose content is not padded to the fixed bucket size must be rejected by the node.
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();
    var recordNodeId = ChannelDht.deriveRecordNodeId(secret);
    KadContent unpadded = new KadContent(now, recordNodeId.exportPublic(), randomBytes(100));
    unpadded.signWith(recordNodeId);
    KademliaId key = ChannelDht.rendezvousKademliaId(secret, now);

    GarlicRouter.handle(node, singleLayerPacket(recordStoreLayer(unpadded)));

    assertThat(node.getKadStoreManager().get(key))
        .as("an invalid (wrong-size) record must not be stored")
        .isNull();
  }

  @Test
  void sizeMismatchWarn_isThrottledToOnePerInterval() {
    // TD022: the size-mismatch drop is the one validation failure logged at WARN (protocol version
    // skew), and since anyone can send cheap wrong-size garbage the WARN must be bounded. The
    // throttle state is a process-global singleton, so probe it with synthetic timestamps far in
    // the future — no other test reaches this region of the clock.
    long base = System.currentTimeMillis() + 1000L * 60 * 60 * 24 * 365;

    assertThat(GarlicRouter.tryAcquireSizeMismatchWarn(base))
        .as("first size mismatch in the window warns")
        .isTrue();
    assertThat(GarlicRouter.tryAcquireSizeMismatchWarn(base + 1))
        .as("immediate repeat is suppressed")
        .isFalse();
    assertThat(
            GarlicRouter.tryAcquireSizeMismatchWarn(
                base + GarlicRouter.SIZE_MISMATCH_WARN_INTERVAL_MS - 1))
        .as("still inside the throttle interval")
        .isFalse();
    assertThat(
            GarlicRouter.tryAcquireSizeMismatchWarn(
                base + GarlicRouter.SIZE_MISMATCH_WARN_INTERVAL_MS))
        .as("interval elapsed, next warn is admitted")
        .isTrue();
  }
}
