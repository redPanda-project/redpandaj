package im.redpanda.dht;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.mailbox.OhId;
import im.redpanda.mailbox.OutboundHandleStore;
import im.redpanda.mailbox.OutboundMailboxStore;
import im.redpanda.mailbox.OutboundService;
import im.redpanda.mailbox.OutboundStore;
import im.redpanda.mailbox.ReturnPath;
import im.redpanda.outbound.v1.MailItem;
import im.redpanda.transport.Peer;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * T162/N5: a client-triggered channel-record lookup must never answer "no record" just because the
 * key is on the anti-loop search blacklist — the joiner's dense lookup schedule and the two parties
 * of a channel look up the same key within 30 s.
 */
class RecordLookupJobBlacklistTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  private ServerContext node;
  private OutboundMailboxStore mailbox;
  private OutboundHandleStore handles;

  @BeforeEach
  void setUp() {
    node = ServerContext.buildDefaultServerContext();
    OutboundStore store = OutboundStore.inMemory();
    handles = store.handles();
    mailbox = store.mailbox();
    node.setOutboundService(new OutboundService(store));
  }

  private static byte[] randomBytes(int len) {
    byte[] bytes = new byte[len];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  /** A hop_count = 0 return path into a fresh local mailbox (the answer lands locally). */
  private ReturnPath localReturnPath() {
    OhId ackOh = OhId.fromBytes(randomBytes(KademliaId.ID_LENGTH_BYTES));
    long now = System.currentTimeMillis();
    handles.put(ackOh, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));
    return new ReturnPath(ackOh, randomBytes(16), List.of());
  }

  private byte[] singleAnswer(ReturnPath returnPath) throws InterruptedException {
    List<MailItem> items = List.of();
    for (int i = 0; i < 200 && items.isEmpty(); i++) {
      items = mailbox.fetchMessages(returnPath.ackOhId(), 10, 0);
      if (items.isEmpty()) {
        Thread.sleep(25);
      }
    }
    assertThat(items).as("exactly one answer per lookup").hasSize(1);
    return items.get(0).getPayload().toByteArray();
  }

  @Test
  void secondLookupWithinTheBlacklistWindow_searchesAndAnswersTheRealRecord() {
    byte[] secret = randomBytes(32);
    long now = System.currentTimeMillis();
    KadContent record =
        ChannelDht.buildRecordContent(secret, randomBytes(ChannelDht.RECORD_SIZE_BYTES), now);
    KademliaId key = ChannelDht.rendezvousKademliaId(secret, now);

    // first lookup for the key: its search puts the key on the blacklist
    new RecordLookupJob(node, key, List.of(localReturnPath())).init();

    // second lookup for the same key a few seconds later (joiner's next step)
    ReturnPath second = localReturnPath();
    RecordLookupJob job = new RecordLookupJob(node, key, List.of(second));
    job.init();
    assertThat(mailbox.fetchMessages(second.ackOhId(), 10, 0))
        .as("must not answer 'no record' without having searched")
        .isEmpty();

    // a peer answers the search with the record → the client gets it
    job.ack(record, new Peer("127.0.0.1", 9500));
    job.success();
    byte[] answer =
        mailbox.fetchMessages(second.ackOhId(), 10, 0).get(0).getPayload().toByteArray();
    assertThat(answer[0]).isEqualTo(RecordLookupJob.RESPONSE_FOUND);
  }

  @Test
  void peerTriggeredSearchForTheKey_staysBlacklisted() {
    KademliaId key = ChannelDht.rendezvousKademliaId(randomBytes(32), System.currentTimeMillis());
    new RecordLookupJob(node, key, List.of(localReturnPath())).init();

    AtomicInteger failed = new AtomicInteger();
    KademliaSearchJobAnswerPeer forwarded =
        new KademliaSearchJobAnswerPeer(node, key, new Peer("127.0.0.1", 9501), 11) {
          @Override
          protected void fail() {
            failed.incrementAndGet();
          }
        };
    forwarded.init();
    assertThat(failed)
        .as("a search another node asked us for must still be cut off (loop protection)")
        .hasValue(1);
  }

  @Test
  void lookupsWaitingForTheSameSearch_shareIt_laterOnesGetAFreshSearch() {
    RecordLookupCoalescer coalescer = new RecordLookupCoalescer();
    KademliaId key = new KademliaId();
    ReturnPath a = localReturnPath();
    ReturnPath b = localReturnPath();
    ReturnPath c = localReturnPath();

    RecordLookupCoalescer.Window window = coalescer.join(key, a, 0);
    assertThat(window).isNotNull();
    assertThat(coalescer.join(key, b, 100)).as("attached while the search waits").isNull();
    assertThat(coalescer.join(key, c, 200)).isNull();
    assertThat(coalescer.searchesScheduled()).isEqualTo(1);

    assertThat(coalescer.close(key, window)).containsExactly(a, b, c);

    // the search has started: a lookup arriving now must not inherit it (it may predate a newer
    // publication of the record) but get its own
    assertThat(coalescer.join(key, localReturnPath(), 300)).isNotNull();
    assertThat(coalescer.searchesScheduled()).isEqualTo(2);
  }

  @Test
  void windowWhoseSearchNeverRan_isReplaced_andAnsweredOnlyOnce() {
    RecordLookupCoalescer coalescer = new RecordLookupCoalescer();
    KademliaId key = new KademliaId();
    ReturnPath a = localReturnPath();
    ReturnPath b = localReturnPath();
    RecordLookupCoalescer.Window stale = coalescer.join(key, a, 0);
    RecordLookupCoalescer.Window fresh = coalescer.join(key, b, RecordLookupCoalescer.MAX_OPEN_MS);
    assertThat(fresh).isNotNull();

    assertThat(coalescer.close(key, fresh)).containsExactly(a, b);
    assertThat(coalescer.close(key, stale))
        .as("a late run of the stale job answers nobody")
        .isEmpty();
  }

  @Test
  void concurrentLookups_eachGetExactlyOneAnswer() throws InterruptedException {
    // End to end through RecordLookupJob.lookup with the real job scheduler: unknown key, no
    // peers → every lookup gets its own not-found answer from the shared search, exactly once.
    KademliaId key = ChannelDht.rendezvousKademliaId(randomBytes(32), System.currentTimeMillis());
    List<ReturnPath> paths = List.of(localReturnPath(), localReturnPath(), localReturnPath());
    for (ReturnPath path : paths) {
      RecordLookupJob.lookup(node, key, path);
    }
    for (ReturnPath path : paths) {
      assertThat(singleAnswer(path)).containsExactly(RecordLookupJob.RESPONSE_NOT_FOUND);
    }
    Thread.sleep(200); // a duplicate answer would land here
    for (ReturnPath path : paths) {
      assertThat(mailbox.fetchMessages(path.ackOhId(), 10, 0)).hasSize(1);
    }
    assertThat(node.getRecordLookupCoalescer().waiting(key)).isZero();
  }
}
