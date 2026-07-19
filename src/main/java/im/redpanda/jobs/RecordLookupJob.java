package im.redpanda.jobs;

import static com.google.protobuf.ByteString.copyFrom;

import im.redpanda.core.KademliaId;
import im.redpanda.core.ServerContext;
import im.redpanda.flaschenpost.ReturnPath;
import im.redpanda.flaschenpost.ReverseGarlic;
import im.redpanda.kademlia.KadContent;
import im.redpanda.outbound.ChannelDht;
import im.redpanda.proto.KademliaStore;
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
  private final ReturnPath returnPath;
  private boolean answered = false;

  RecordLookupJob(ServerContext serverContext, KademliaId searchedKey, ReturnPath returnPath) {
    super(serverContext, searchedKey);
    this.searchedKey = searchedKey;
    this.returnPath = returnPath;
  }

  /**
   * Resolves the rendezvous record for {@code searchedKey}: local KadStore first (no delay), then a
   * randomized delayed DHT search. Exactly one reverse-garlic answer is sent.
   */
  public static void lookup(
      ServerContext serverContext, KademliaId searchedKey, ReturnPath returnPath) {
    long now = System.currentTimeMillis();
    KadContent local = serverContext.getKadStoreManager().get(searchedKey);
    if (local != null && ChannelDht.extractNewest(List.of(local), searchedKey, now) != null) {
      respond(serverContext, searchedKey, returnPath, local);
      return;
    }
    new DelayedSearchJob(serverContext, searchedKey, returnPath).start();
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

  private synchronized void fireAnswer(KadContent record) {
    if (!answered) {
      answered = true;
      respond(serverContext, searchedKey, returnPath, record);
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
    private final ReturnPath returnPath;

    DelayedSearchJob(ServerContext serverContext, KademliaId searchedKey, ReturnPath returnPath) {
      super(serverContext, rand.nextLong(LOOKUP_DELAY_JITTER_MS + 1), false, true);
      this.searchedKey = searchedKey;
      this.returnPath = returnPath;
    }

    @Override
    public void init() {
      // no setup needed
    }

    @Override
    public void work() {
      try {
        new RecordLookupJob(serverContext, searchedKey, returnPath).start();
        logger.debug("started delayed channel record lookup search");
      } finally {
        done();
      }
    }
  }
}
