package im.redpanda.outbound;

import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.kademlia.KadContent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
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
   * size leaks nothing (per the spec's single-padding-bucket decision). The content is opaque:
   * {@code [4 ciphertextLen][ciphertext][random padding]} up to this size.
   */
  public static final int RECORD_SIZE_BYTES = 512;

  /** Length prefix (bytes) in front of the opaque ciphertext inside the padded record. */
  public static final int CIPHERTEXT_LEN_PREFIX = 4;

  /** Largest ciphertext that still fits the fixed record after the length prefix. */
  public static final int MAX_CIPHERTEXT_BYTES = RECORD_SIZE_BYTES - CIPHERTEXT_LEN_PREFIX;

  /**
   * Maximum age of a rendezvous record before it is considered stale. The spec sets a 48 h TTL;
   * records rotate under the UTC-day key, so a record published late yesterday must stay usable
   * across today, hence 48 h plus a small rotation slack.
   */
  public static final long MAX_RECORD_AGE_MS = 1000L * 60 * 60 * (48 + 2); // 48h TTL + 2h slack

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

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
   * Builds the signed, fixed-size rendezvous KadContent for a channel. {@code ciphertext} is the
   * already k_enc-encrypted channel state; it is framed as {@code [4 len][ciphertext][random
   * padding]} and signed with the derived record key. Reference for the client-side build (T44).
   *
   * @throws IllegalArgumentException if the ciphertext does not fit the fixed record
   */
  public static KadContent buildRecordContent(byte[] channelSecret, byte[] ciphertext, long nowMs) {
    byte[] padded = padToFixedSize(ciphertext);
    NodeId recordNodeId = deriveRecordNodeId(channelSecret);
    KadContent kadContent = new KadContent(nowMs, recordNodeId.exportPublic(), padded);
    kadContent.signWith(recordNodeId);
    return kadContent;
  }

  /** Frames and pads the opaque ciphertext to exactly {@link #RECORD_SIZE_BYTES}. */
  private static byte[] padToFixedSize(byte[] ciphertext) {
    if (ciphertext.length > MAX_CIPHERTEXT_BYTES) {
      throw new IllegalArgumentException(
          "channel record ciphertext too large: "
              + ciphertext.length
              + " > "
              + MAX_CIPHERTEXT_BYTES);
    }
    ByteBuffer buffer = ByteBuffer.allocate(RECORD_SIZE_BYTES);
    buffer.putInt(ciphertext.length);
    buffer.put(ciphertext);
    byte[] padding = new byte[buffer.remaining()];
    SECURE_RANDOM.nextBytes(padding);
    buffer.put(padding);
    return buffer.array();
  }

  /**
   * Extracts the opaque ciphertext from a padded record content, or {@code null} if the framing is
   * malformed. Used by clients (T44) after a successful lookup; nodes never need this.
   */
  public static byte[] extractCiphertext(byte[] recordContent) {
    if (recordContent == null || recordContent.length != RECORD_SIZE_BYTES) {
      return null;
    }
    ByteBuffer buffer = ByteBuffer.wrap(recordContent);
    int len = buffer.getInt();
    if (len < 0 || len > buffer.remaining()) {
      return null;
    }
    byte[] ciphertext = new byte[len];
    buffer.get(ciphertext);
    return ciphertext;
  }

  /**
   * Validates a single record as accepted for storage / serving: signed by the embedded pubkey
   * (self-certifying — the pubkey pins the Kademlia key), exactly the fixed padded size (uniformity
   * is part of the anti-profiling contract) and not older than {@link #MAX_RECORD_AGE_MS}. The
   * content stays opaque — the node deliberately cannot tell which channel it belongs to.
   */
  public static boolean isValidRecord(KadContent content, long nowMs) {
    if (content == null || content.getContent() == null) {
      return false;
    }
    if (content.getContent().length != RECORD_SIZE_BYTES) {
      return false;
    }
    if (nowMs - content.getTimestamp() > MAX_RECORD_AGE_MS) {
      return false;
    }
    return content.verify();
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
