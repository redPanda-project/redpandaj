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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  void concurrentLookups_shareOneSearch_eachGetExactlyOneAnswer() throws Exception {
    // End to end through RecordLookupJob.lookup with the real job scheduler: unknown key, no
    // peers; three lookups arrive from three threads at once → one scheduled search, and every
    // lookup gets its own not-found answer from it, exactly once.
    KademliaId key = ChannelDht.rendezvousKademliaId(randomBytes(32), System.currentTimeMillis());
    List<ReturnPath> paths = List.of(localReturnPath(), localReturnPath(), localReturnPath());
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(paths.size());
    try {
      List<Future<?>> calls = new ArrayList<>();
      for (ReturnPath path : paths) {
        calls.add(
            pool.submit(
                () -> {
                  go.await();
                  RecordLookupJob.lookup(node, key, path);
                  return null;
                }));
      }
      go.countDown();
      for (Future<?> call : calls) {
        call.get(10, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    // The window stays open for the 0–1.5 s jitter; the three calls arrive within microseconds.
    assertThat(node.getRecordLookupCoalescer().searchesScheduled())
        .as("one search for all lookups that arrived while it waited")
        .isEqualTo(1);
    for (ReturnPath path : paths) {
      assertThat(singleAnswer(path)).containsExactly(RecordLookupJob.RESPONSE_NOT_FOUND);
    }
    Thread.sleep(200); // a duplicate answer would land here
    for (ReturnPath path : paths) {
      assertThat(mailbox.fetchMessages(path.ackOhId(), 10, 0)).hasSize(1);
    }
    assertThat(node.getRecordLookupCoalescer().waiting(key)).isZero();
  }

  @Test
  void k5_concurrentJoinsWhileAnotherThreadCloses_everyLookupInExactlyOneSearch() throws Exception {
    RecordLookupCoalescer coalescer = new RecordLookupCoalescer();
    KademliaId key = new KademliaId();
    int lookups = 48;
    List<ReturnPath> paths = new ArrayList<>();
    for (int i = 0; i < lookups; i++) {
      paths.add(localReturnPath());
    }
    ConcurrentLinkedQueue<RecordLookupCoalescer.Window> opened = new ConcurrentLinkedQueue<>();
    List<ReturnPath> answered = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch go = new CountDownLatch(1);
    AtomicBoolean joinsDone = new AtomicBoolean();
    ExecutorService pool = Executors.newFixedThreadPool(lookups + 1);
    try {
      List<Future<?>> joins = new ArrayList<>();
      for (ReturnPath path : paths) {
        joins.add(
            pool.submit(
                () -> {
                  go.await();
                  RecordLookupCoalescer.Window window =
                      coalescer.join(key, path, System.currentTimeMillis());
                  if (window != null) {
                    opened.add(window);
                  }
                  return null;
                }));
      }
      Future<?> closer =
          pool.submit(
              () -> {
                go.await();
                while (!joinsDone.get() || !opened.isEmpty()) {
                  RecordLookupCoalescer.Window next = opened.poll();
                  if (next != null) {
                    answered.addAll(coalescer.close(key, next));
                  }
                }
                return null;
              });
      go.countDown();
      for (Future<?> join : joins) {
        join.get(10, TimeUnit.SECONDS);
      }
      joinsDone.set(true);
      closer.get(10, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
    assertThat(answered)
        .as("every lookup is answered by exactly one search")
        .hasSize(lookups)
        .containsExactlyInAnyOrderElementsOf(paths);
    assertThat(coalescer.waiting(key)).isZero();
  }

  @Test
  void k4_aReturnPathThatThrows_doesNotCostTheOthersTheirAnswer() {
    // The deposit of the answer into the first lookup's ack mailbox throws (any runtime failure
    // while answering one return path); the second lookup must still get its answer.
    OutboundStore store = OutboundStore.inMemory();
    OhId brokenOh = OhId.fromBytes(randomBytes(KademliaId.ID_LENGTH_BYTES));
    node.setOutboundService(
        new OutboundService(store) {
          @Override
          public DepositResult depositMessage(OhId ohId, byte[] payload, byte[] sessionTag) {
            if (ohId.equals(brokenOh)) {
              throw new IllegalStateException("answer for this lookup fails");
            }
            return super.depositMessage(ohId, payload, sessionTag);
          }
        });
    long now = System.currentTimeMillis();
    OhId goodOh = OhId.fromBytes(randomBytes(KademliaId.ID_LENGTH_BYTES));
    store
        .handles()
        .put(goodOh, new OutboundHandleStore.HandleRecord(new byte[65], now, now + 60_000));
    ReturnPath broken = new ReturnPath(brokenOh, randomBytes(16), List.of());
    ReturnPath good = new ReturnPath(goodOh, randomBytes(16), List.of());

    KademliaId key = ChannelDht.rendezvousKademliaId(randomBytes(32), now);
    RecordLookupJob job = new RecordLookupJob(node, key, List.of(broken, good));
    job.init();
    job.fail(); // no record found → answer every lookup with not-found

    List<MailItem> items = store.mailbox().fetchMessages(goodOh, 10, 0);
    assertThat(items).as("the lookup after the failing one is still answered").hasSize(1);
    assertThat(items.get(0).getPayload().toByteArray())
        .containsExactly(RecordLookupJob.RESPONSE_NOT_FOUND);
  }
}
