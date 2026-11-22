package im.redpanda.dht;

import static com.google.protobuf.ByteString.copyFrom;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.mailbox.OhResolveJob;
import im.redpanda.mailbox.ReturnPath;
import im.redpanda.ops.Job;
import im.redpanda.proto.KademliaStore;
import im.redpanda.routing.ReverseGarlic;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * T43: resolves a channel-rendezvous record on behalf of a DHT-fremd light client and returns it
 * via the client-chosen {@link ReturnPath} (reverse garlic). Mirrors {@link OhResolveJob}: the
 * local KadStore is checked first (no delay, a local read leaks nothing), otherwise a randomized
 * delayed DHT search is issued. Whatever the outcome, exactly one reverse-garlic answer is sent so
 * the client never waits out a timeout for a record that simply does not exist yet.
 *
 * <p>The answer payload is framed as {@code [1 status][KademliaStore proto if found]}: {@code
 * status = 1} carries the newest valid record ({@link ChannelDht#extractNewest}) so the client can
 * re-verify it self-certifyingly and decrypt it; {@code status = 0} means "no record", nothing
 * follows.
 */
public class RecordLookupJob extends KademliaSearchJob {

  private static final Logger logger = LogManager.getLogger();

  /** Answer status byte: no valid record found for the key. */
  public static final byte RESPONSE_NOT_FOUND = 0x00;

  /** Answer status byte: a valid record follows as a serialized {@link KademliaStore}. */
  public static final byte RESPONSE_FOUND = 0x01;

  /** Upper bound for the randomized delay before the DHT search is issued (anti-profiling). */
  static final long LOOKUP_DELAY_JITTER_MS = Duration.ofMillis(1500).toMillis();

  private final KademliaId searchedKey;
  private final List<ReturnPath> returnPaths;
  private boolean answered = false;

  /** One search for {@code searchedKey}, answering every return path in {@code returnPaths}. */
  RecordLookupJob(
      ServerContext serverContext, KademliaId searchedKey, List<ReturnPath> returnPaths) {
    super(serverContext, searchedKey);
    this.searchedKey = searchedKey;
    this.returnPaths = List.copyOf(returnPaths);
  }

  /**
   * Resolves the rendezvous record for {@code searchedKey}: local KadStore first (no delay), then a
   * randomized delayed DHT search — shared with other lookups for the same key that arrive before
   * that search starts (T162/N5, see {@link RecordLookupCoalescer}). Exactly one reverse-garlic
   * answer is sent per lookup.
   */
  public static void lookup(
      ServerContext serverContext, KademliaId searchedKey, ReturnPath returnPath) {
    long now = System.currentTimeMillis();
    KadContent local = serverContext.getKadStoreManager().get(searchedKey);
    if (local != null && ChannelDht.extractNewest(List.of(local), searchedKey, now) != null) {
      respond(serverContext, searchedKey, returnPath, local);
      return;
    }
    RecordLookupCoalescer coalescer = serverContext.getRecordLookupCoalescer();
    RecordLookupCoalescer.Window window = coalescer.join(searchedKey, returnPath, now);
    if (window == null) {
      return; // attached to the search already scheduled for this key
    }
    try {
      new DelayedSearchJob(serverContext, searchedKey, window).start();
    } catch (RuntimeException e) {
      // scheduling failed (executor shutting down): close the window so later lookups open a new
      // one; the attached lookups get no answer, as before when the job could not be scheduled
      coalescer.close(searchedKey, window);
      logger.debug("could not schedule channel record lookup: {}", e.getMessage());
    }
  }

  @Override
  protected ArrayList<KadContent> success() {
    ArrayList<KadContent> contents = super.success();
    KadContent record = ChannelDht.extractNewest(contents, searchedKey, System.currentTimeMillis());
    fireAnswer(record);
    return contents;
  }

  @Override
  protected void fail() {
    fireAnswer(null);
  }

  /**
   * T162/N5: this lookup is the node's own search for a client, never part of a search loop between
   * nodes (those run as {@link KademliaSearchJobAnswerPeer} and stay blacklisted). Before, a second
   * lookup for the same key within 30 s — the two parties of a channel, a joiner's dense lookup
   * schedule, the today/yesterday keys of a repeated recovery — was failed in {@code init()}
   * without asking anyone and answered {@code RESPONSE_NOT_FOUND}: a false "no record" that made
   * the client believe its counterpart absent. The key is still blacklisted.
   */
  @Override
  protected boolean rejectsBlacklistedKey() {
    return false;
  }

  private synchronized void fireAnswer(KadContent record) {
    if (!answered) {
      answered = true;
      for (ReturnPath returnPath : returnPaths) {
        // review K4: a failure for one return path must not cost the other lookups their answer
        try {
          respond(serverContext, searchedKey, returnPath, record);
        } catch (RuntimeException e) {
          logger.debug("could not answer a channel record lookup: {}", e.getMessage());
        }
      }
    }
  }

  /** Frames the answer and sends it back through the return path. {@code record} may be null. */
  static void respond(
      ServerContext serverContext,
      KademliaId searchedKey,
      ReturnPath returnPath,
      KadContent record) {
    KadContent valid =
        record == null
            ? null
            : ChannelDht.extractNewest(List.of(record), searchedKey, System.currentTimeMillis());
    byte[] payload;
    if (valid == null) {
      payload = new byte[] {RESPONSE_NOT_FOUND};
    } else {
      byte[] store =
          KademliaStore.newBuilder()
              .setTimestamp(valid.getTimestamp())
              .setPublicKey(copyFrom(valid.getPubkey()))
              .setContent(copyFrom(valid.getContent()))
              .setSignature(copyFrom(valid.getSignature()))
              .build()
              .toByteArray();
      payload = ByteBuffer.allocate(1 + store.length).put(RESPONSE_FOUND).put(store).array();
    }
    ReverseGarlic.sendTaggedPayload(serverContext, returnPath, payload);
    logger.debug("answered channel record lookup, found={}", valid != null);
  }

  /** One-shot job adding the randomized anti-profiling delay before the actual DHT search. */
  static class DelayedSearchJob extends Job {

    private final KademliaId searchedKey;
    private final RecordLookupCoalescer.Window window;

    DelayedSearchJob(
        ServerContext serverContext, KademliaId searchedKey, RecordLookupCoalescer.Window window) {
      super(serverContext, rand.nextLong(LOOKUP_DELAY_JITTER_MS + 1), false, true);
      this.searchedKey = searchedKey;
      this.window = window;
    }

    @Override
    public void init() {
      // no setup needed
    }

    @Override
    public void work() {
      try {
        // the window closes now: lookups arriving from here on get their own, newer search
        List<ReturnPath> returnPaths =
            serverContext.getRecordLookupCoalescer().close(searchedKey, window);
        if (returnPaths.isEmpty()) {
          return; // taken over by a newer window
        }
        new RecordLookupJob(serverContext, searchedKey, returnPaths).start();
        logger.debug(
            "started delayed channel record lookup search for {} lookup(s)", returnPaths.size());
      } finally {
        done();
      }
    }
  }
}
