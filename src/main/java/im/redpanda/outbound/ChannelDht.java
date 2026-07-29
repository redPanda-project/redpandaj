package im.redpanda.outbound;

import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.kademlia.KadContent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.List;

/**
 * T43 channel rendezvous DHT primitives (multi-OH / DHT-rendezvous, {@code
 * plans/PLAN-multi-oh-dht.md}).
 *
 * <p>A channel is a keypair; QR v4 shares the 32-byte channel secret, so every participant — and
 * only they — can compute the same derived rendezvous keypair, hence the same daily-rotating
 * Kademlia key, sign records and decrypt their content. The rendezvous record maps a channel to its
 * participants' current OH lists so a channel heals purely over the DHT even when all host nodes
 * are unreachable.
 *
 * <p>Key derivation (mirrors {@link OhDht}, but seeded from the channel <em>secret</em> instead of
 * a public identifier): {@code recordNodeId = NodeId.fromSeed(SHA256(DOMAIN_TAG ||
 * channelSecret))}. Seeding from the secret is deliberate — the channel public key is embedded in
 * every stored record for signature verification, so deriving the signing key from anything public
 * would let any observer forge records. Only holders of the channel secret can therefore publish or
 * overwrite a record. The Kademlia key is the standard self-certifying {@code H(dateUTC ||
 * recordPubkey)}; the domain tag lives in the derived keypair's namespace, keeping it disjoint from
 * node ids and OH announce records.
 *
 * <p>The record content is an <em>opaque ciphertext</em> to nodes: it is encrypted with {@code
 * k_enc = HKDF(channel_secret)} and carries the participant list, names and per-participant OH
 * lists (newest-wins per participant entry via an inner timestamp — a client-side merge, see T44).
 * Nodes never parse it; they only enforce the self-certifying signature, the fixed padded size, the
 * 48 h TTL and a global store rate limit.
 *
 * <p>Anti-profiling: records are padded to a fixed {@link #RECORD_SIZE_BYTES} so stored/answered
 * sizes never reveal which channel is being published or resolved. Store and lookup travel
 * garlic-wrapped to a <em>remote</em> node (the light client stays DHT-fremd), so the directly
 * connected node does not learn the query interest (see {@code GarlicRouter} record commands and
 * the T43 spec decisions).
 */
public final class ChannelDht {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /**
   * Domain separation tag: rendezvous record keys live in their own namespace, distinct from node
   * ids ({@link OhDht} uses {@code redpanda.oh.announce.v1}).
   */
  private static final byte[] DOMAIN_TAG =
      "redpanda.channel.rendezvous.v1".getBytes(StandardCharsets.UTF_8);

  /**
   * Fixed serialized size of every rendezvous record content. One bucket for all channels so record
   * size leaks nothing (per the spec's single-padding-bucket decision). The content is fully opaque
   * to nodes: the client's {@code [nonce][AEAD ciphertext of a fixed-size plaintext]} blob, sized
   * to exactly this many bytes. All length/padding metadata lives <em>inside</em> the encrypted
   * plaintext, so no cleartext field ever reveals the real payload size.
   *
   * <p>Sized 1024 since the OH redundancy went to k=3: the usable plaintext is {@code
   * RECORD_SIZE_BYTES - 12 nonce - 16 GCM tag}, and two participants with three OH descriptors each
   * (~74 bytes per descriptor) need 530..574 bytes, which no longer fit the previous 512-byte
   * bucket. 1024 leaves headroom for IPv6 endpoints and a third participant.
   *
   * <p>Changing this value is a <em>breaking</em> protocol change: {@link #isValidRecord} rejects
   * every record of a different size, so nodes on the old bucket drop records published by upgraded
   * clients and vice versa. Roll the network out before the clients; channels whose record is
   * dropped in the meantime keep healing over the in-band {@code oh_update} announce.
   */
  public static final int RECORD_SIZE_BYTES = 1024;

  /**
   * Maximum age of a rendezvous record before it is considered stale. The spec sets a 48 h TTL;
   * records rotate under the UTC-day key, so a record published late yesterday must stay usable
   * across today, hence 48 h plus a small rotation slack.
   */
  public static final long MAX_RECORD_AGE_MS = 1000L * 60 * 60 * (48 + 2); // 48h TTL + 2h slack

  /**
   * Tolerated clock skew for records dated ahead of now. Records further in the future are rejected
   * so a clock-skewed (or malicious) peer cannot make a future-dated record win the newest-wins
   * selection. Mirrors {@link im.redpanda.kademlia.KadStoreManager}'s 15-minute future bound.
   */
  public static final long MAX_FUTURE_SKEW_MS = 1000L * 60 * 15; // 15 min

  private ChannelDht() {}

  /**
   * Derives the deterministic rendezvous record NodeId for a channel: {@code seed = SHA256(tag ||
   * channelSecret)} → {@link NodeId#fromSeed}. Same channel secret → same keypair on every device.
   * Only holders of the channel secret can compute (and therefore sign with) this key.
   */
  public static NodeId deriveRecordNodeId(byte[] channelSecret) {
    ByteBuffer seedInput = ByteBuffer.allocate(DOMAIN_TAG.length + channelSecret.length);
    seedInput.put(DOMAIN_TAG).put(channelSecret);
    byte[] seed = Sha256Hash.create(seedInput.array()).getBytes();
    return NodeId.fromSeed(seed);
  }

  /**
   * The Kademlia key the rendezvous record for this channel lives under at {@code timestampMs} (the
   * key rotates with the UTC date, like all KadContent ids).
   */
  public static KademliaId rendezvousKademliaId(byte[] channelSecret, long timestampMs) {
    return KadContent.createKademliaId(
        timestampMs, deriveRecordNodeId(channelSecret).exportPublic());
  }

  /**
   * Builds the signed rendezvous KadContent for a channel. {@code opaqueContent} is the client's
   * already-encrypted, fixed-size record blob (nonce + AEAD ciphertext of a fixed-size plaintext);
   * it must be exactly {@link #RECORD_SIZE_BYTES} bytes and is signed with the derived record key.
   * Reference for the client-side build (T44). Nodes never parse the content.
   *
   * @throws IllegalArgumentException if {@code opaqueContent} is not exactly the fixed bucket size
   */
  public static KadContent buildRecordContent(
      byte[] channelSecret, byte[] opaqueContent, long nowMs) {
    if (opaqueContent.length != RECORD_SIZE_BYTES) {
      throw new IllegalArgumentException(
          "channel record content must be exactly "
              + RECORD_SIZE_BYTES
              + " bytes, was "
              + opaqueContent.length);
    }
    NodeId recordNodeId = deriveRecordNodeId(channelSecret);
    KadContent kadContent =
        new KadContent(nowMs, recordNodeId.exportPublic(), opaqueContent.clone());
    kadContent.signWith(recordNodeId);
    return kadContent;
  }

  /**
   * Distinct outcomes of {@link #validateRecord}. Callers that drop invalid records should log the
   * reasons separately: {@link #WRONG_SIZE} indicates a client publishing a different {@link
   * #RECORD_SIZE_BYTES} bucket — i.e. protocol-version skew between client and node (TD022), a
   * deployment error worth surfacing — whereas the other reasons are routine input validation that
   * any anonymous sender can trigger and must stay quiet.
   */
  public enum RecordValidation {
    VALID,
    /** No record or no content bytes at all (malformed input). */
    MISSING_CONTENT,
    /**
     * Content is not exactly {@link #RECORD_SIZE_BYTES}: the sender speaks a different record
     * format version (the size is a breaking protocol constant, see {@link #RECORD_SIZE_BYTES}).
     * Checked before the signature, so this outcome is cheap for anyone to trigger.
     */
    WRONG_SIZE,
    /** Older than {@link #MAX_RECORD_AGE_MS}. */
    EXPIRED,
    /** Dated further than {@link #MAX_FUTURE_SKEW_MS} ahead of now. */
    FUTURE_DATED,
    /** Signature does not verify against the embedded pubkey. */
    BAD_SIGNATURE
  }

  /**
   * Validates a single record as accepted for storage / serving: signed by the embedded pubkey
   * (self-certifying — the pubkey pins the Kademlia key), exactly the fixed bucket size (uniformity
   * is part of the anti-profiling contract), not older than {@link #MAX_RECORD_AGE_MS} and not
   * further than {@link #MAX_FUTURE_SKEW_MS} in the future. The content stays opaque — the node
   * deliberately cannot tell which channel it belongs to.
   *
   * @return {@link RecordValidation#VALID}, or the first failed check in the order missing content
   *     → size → age → future skew → signature (cheap checks first; the signature check is the only
   *     expensive one)
   */
  public static RecordValidation validateRecord(KadContent content, long nowMs) {
    if (content == null || content.getContent() == null) {
      return RecordValidation.MISSING_CONTENT;
    }
    if (content.getContent().length != RECORD_SIZE_BYTES) {
      return RecordValidation.WRONG_SIZE;
    }
    if (nowMs - content.getTimestamp() > MAX_RECORD_AGE_MS) {
      return RecordValidation.EXPIRED;
    }
    if (content.getTimestamp() - nowMs > MAX_FUTURE_SKEW_MS) {
      return RecordValidation.FUTURE_DATED;
    }
    return content.verify() ? RecordValidation.VALID : RecordValidation.BAD_SIGNATURE;
  }

  /** Boolean convenience over {@link #validateRecord} for callers that don't need the reason. */
  public static boolean isValidRecord(KadContent content, long nowMs) {
    return validateRecord(content, nowMs) == RecordValidation.VALID;
  }

  /**
   * Picks the newest valid record for {@code searchedKey} from DHT results. Besides {@link
   * #isValidRecord} the record's self-certifying id must equal the searched key, so a peer cannot
   * smuggle a foreign (but validly signed) record into the answer. Newest timestamp wins (a channel
   * republishes daily and on every OH change).
   *
   * @return the newest qualifying record, or {@code null} if none qualifies
   */
  public static KadContent extractNewest(
      List<KadContent> contents, KademliaId searchedKey, long nowMs) {
    if (contents == null || contents.isEmpty()) {
      return null;
    }
    KadContent best = null;
    for (KadContent content : contents) {
      if (content == null || !isValidRecord(content, nowMs)) {
        continue;
      }
      if (!content.getId().equals(searchedKey)) {
        continue;
      }
      if (best == null || content.getTimestamp() > best.getTimestamp()) {
        best = content;
      }
    }
    return best;
  }
}
