package im.redpanda.mailbox;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * T162: a non-host final garlic hop resolves the target OH through the DHT for every deposit it
 * forwards (MS02b). A reverse-garlic reply and the R-ACK of the message it answers both target the
 * same OH (the channel's first own OH) and often leave the onion at the same node, a few
 * milliseconds apart. The anti-loop search blacklist of {@code KademliaSearchJob} (30 s per key)
 * used to reject the second lookup outright: {@code onFailed} fired without a single peer being
 * asked, the deposit was parked for 15 s, the retry hit the same blacklist entry and the deposit
 * was dropped with {@code HANDLE_EXPIRED} — the testnet's "status 2 after 15–35 s".
 */
class OhResolveJobBlacklistTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  private static OhId randomOhId() {
    byte[] bytes = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(bytes);
    return OhId.fromBytes(bytes);
  }

  @Test
  void secondResolveOfSameOhWithinBlacklistWindow_isNotFailedWithoutAsking() {
    ServerContext node = ServerContext.buildDefaultServerContext();
    OhId ohId = randomOhId();
    AtomicInteger failed = new AtomicInteger();

    // first deposit for the OH (e.g. the R-ACK of the previous message): network lookup starts
    OhResolveJob first = new OhResolveJob(node, ohId, record -> {}, failed::incrementAndGet);
    first.init();
    assertThat(failed).hasValue(0);

    // second deposit for the same OH a few ms later (the reverse-garlic reply)
    OhResolveJob second = new OhResolveJob(node, ohId, record -> {}, failed::incrementAndGet);
    second.init();

    assertThat(failed)
        .as(
            "a concurrent resolve of the same OH must run its own lookup, not fail on the"
                + " anti-loop blacklist of recursive peer searches")
        .hasValue(0);
  }

  @Test
  void ohLookup_stillBlacklistsItsKey_forPeerTriggeredSearches() {
    // N4: the exemption must cover only the lookup itself. After an OhResolveJob for key K, a
    // peer asking us for K (KADEMLIA_GET we cannot answer) must not start a recursive search.
    ServerContext node = ServerContext.buildDefaultServerContext();
    OhId ohId = randomOhId();
    KademliaId key = OhDht.announceKademliaId(ohId, System.currentTimeMillis());
    new OhResolveJob(node, ohId, record -> {}, () -> {}).init();

    AtomicInteger failed = new AtomicInteger();
    im.redpanda.dht.KademliaSearchJobAnswerPeer answer =
        new im.redpanda.dht.KademliaSearchJobAnswerPeer(
            node, key, new im.redpanda.transport.Peer("127.0.0.1", 9400), 7) {
          @Override
          protected void fail() {
            failed.incrementAndGet();
          }
        };
    answer.init();

    assertThat(failed)
        .as("a peer-triggered search for a key our own OH lookup holds must be cut off")
        .hasValue(1);
  }

  @Test
  void recursivePeerSearch_staysBlacklisted() {
    // The blacklist itself is still needed against search loops between nodes: a peer-triggered
    // search for a key this node is already searching must not start a second one.
    ServerContext node = ServerContext.buildDefaultServerContext();
    KademliaId key = OhDht.announceKademliaId(randomOhId(), System.currentTimeMillis());
    AtomicInteger failed = new AtomicInteger();

    new im.redpanda.dht.KademliaSearchJob(node, key).init();
    im.redpanda.dht.KademliaSearchJob loop =
        new im.redpanda.dht.KademliaSearchJob(node, key) {
          @Override
          protected void fail() {
            failed.incrementAndGet();
          }
        };
    loop.init();

    assertThat(failed).hasValue(1);
  }
}
