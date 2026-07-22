package im.redpanda.flaschenpost;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import im.redpanda.core.KademliaId;
import im.redpanda.core.ServerContext;
import im.redpanda.jobs.RecordLookupJob;
import im.redpanda.kademlia.KadContent;
import im.redpanda.outbound.ChannelDht;
import im.redpanda.outbound.OutboundHandleStore;
import im.redpanda.outbound.OutboundMailboxStore;
import im.redpanda.outbound.OutboundService;
import im.redpanda.outbound.v1.MailItem;
import im.redpanda.proto.KademliaStore;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

/**
 * T43 acceptance: a garlic-wrapped {@code CMD_RECORD_STORE} stores a channel-rendezvous record in
 * the node's DHT, and a garlic-wrapped {@code CMD_RECORD_LOOKUP} resolves it and returns it via the
 * client-chosen return path (reverse garlic into the client's own OH mailbox). Uses a hop_count = 0
 * return path so the answer is deposited locally and deterministically, mirroring the MS06 R-ACK
 * router test. Also covers the not-found answer and that an invalid record is not stored.
 */
public class RecordDhtRouterTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  private ServerContext node;
  private OutboundMailboxStore mailbox;
  private OutboundHandleStore handles;
  private byte[] ackOhId;
  private byte[] ackSessionTag;

  @Before
  public void setUp() {
    node = ServerContext.buildDefaultServerContext();
    handles = new OutboundHandleStore();
    mailbox = new OutboundMailboxStore();
    node.setOutboundService(new OutboundService(handles, mailbox));

    ackOhId = randomBytes(KademliaId.ID_LENGTH_BYTES);
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
            node.getNodeId().getEncryptionPubKey(), node.getNonce(), plaintext);
    return FlaschenpostV2.buildPacket(packetId, node.getNonce(), body);
  }

  @Test
  public void storeThenLookup_roundTripsRecordBackThroughReturnPath() throws Exception {
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
  public void lookup_unknownKey_returnsNotFound() throws Exception {
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
  private List<MailItem> awaitMailbox(byte[] ohId) throws InterruptedException {
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
  public void lookup_rateLimitExhausted_dropsWithoutSearching() throws Exception {
    // Swap in a 1-token bucket with a refill interval far longer than the lookup job's own
    // anti-profiling jitter (up to 1.5 s, see RecordLookupJob.LOOKUP_DELAY_JITTER_MS) so the second
    // call is deterministically over budget regardless of how long the first answer takes.
    RecordStoreRateLimiter previous =
        GarlicRouter.swapRecordLookupRateLimiterForTest(
            new RecordStoreRateLimiter(1, 60_000L, System.currentTimeMillis()));
    try {
      KademliaId key =
          ChannelDht.rendezvousKademliaId(randomChannelSecret(), System.currentTimeMillis());
      byte[] layer = recordLookupLayer(key, zeroHopReturnPath());

      // 1st lookup consumes the single token and is admitted (answers not-found asynchronously).
      // 2nd lookup arrives immediately after → bucket empty → dropped before a search ever starts.
      // Fixed, distinct packet IDs (rather than the default random ones) so this can never be
      // confused with a GMStoreManager packet_id-dedup drop.
      GarlicRouter.handle(node, singleLayerPacket(layer, 1));
      GarlicRouter.handle(node, singleLayerPacket(layer, 2));

      List<MailItem> items = awaitMailbox(ackOhId);
      // Both lookups share the same up-to-1.5 s jitter window (RecordLookupJob), so the 1st
      // answer landing first proves nothing about the 2nd being dropped — settle past the max
      // jitter before asserting the final count, so a regression that wrongly admits the 2nd
      // lookup can't slip through as a late-arriving 2nd item.
      Thread.sleep(1_700L); // > RecordLookupJob.LOOKUP_DELAY_JITTER_MS (1.5 s), package-private
      items = mailbox.fetchMessages(ackOhId, 10, 0);
      assertThat(items)
          .as("only the 1st (admitted) lookup may produce an answer, the 2nd must be dropped")
          .hasSize(1);
    } finally {
      GarlicRouter.swapRecordLookupRateLimiterForTest(previous);
    }
  }

  @Test
  public void store_invalidRecord_isNotStored() throws Exception {
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
}
