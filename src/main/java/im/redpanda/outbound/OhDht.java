package im.redpanda.outbound;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.kademlia.KadContent;
import im.redpanda.outbound.v1.OhNodeRecord;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Arrays;
import java.util.List;

/**
 * MS02b OH → host-node discovery primitives.
 *
 * <p>The DHT only stores self-certifying records: the Kademlia key is {@code H(dateUTC || pubkey)}
 * and content is signature-checked against the embedded pubkey. To announce {@code oh_id → host
 * node} without protocol changes, the announce keypair is <em>derived deterministically from the
 * oh_id</em> (seed = SHA256(domain tag || oh_id) → Ed25519/X25519 NodeId, see {@link
 * NodeId#fromSeed}). Everyone who knows the oh_id — and only they — can compute the same pubkey,
 * hence the same daily-rotating Kademlia key, and verify the record signature.
 *
 * <p>Trade-off (documented in the milestone): knowing an oh_id is already the capability to deposit
 * into the mailbox; with the derived key it additionally allows publishing/overwriting the announce
 * record (newest timestamp wins). Authenticated announces (e.g. binding the record to the
 * registered oh_auth key) are deferred.
 *
 * <p>Anti-profiling: records are padded to a fixed {@link #RECORD_SIZE_BYTES} so stored/answered
 * record sizes do not reveal which OH is being announced or resolved; announce and lookup jobs add
 * randomized delays (see {@code OhAnnounceJob} / {@code OhResolveJob}).
 */
public final class OhDht {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /** Domain separation tag: announce keys live in their own namespace, distinct from node ids. */
  private static final byte[] DOMAIN_TAG =
      "redpanda.oh.announce.v1".getBytes(StandardCharsets.UTF_8);

  /** Fixed serialized size of every {@link OhNodeRecord} (padding field fills the remainder). */
  public static final int RECORD_SIZE_BYTES = 256;

  /** Maximum age of an announce record before it is considered stale at resolve time. */
  public static final long MAX_RECORD_AGE_MS = 1000L * 60 * 60 * 26; // 26h: daily rotation + slack

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private OhDht() {}

  /**
   * Derives the deterministic announce NodeId for an oh_id: {@code seed = SHA256(tag || oh_id)} →
   * {@link NodeId#fromSeed}. Same oh_id → same keypair on every node.
   */
  public static NodeId deriveAnnounceNodeId(byte[] ohId) {
    ByteBuffer seedInput = ByteBuffer.allocate(DOMAIN_TAG.length + ohId.length);
    seedInput.put(DOMAIN_TAG).put(ohId);
    byte[] seed = Sha256Hash.create(seedInput.array()).getBytes();

    return NodeId.fromSeed(seed);
  }

  /**
   * The Kademlia key the announce record for this oh_id lives under at {@code timestampMs} (the key
   * rotates with the UTC date, like all KadContent ids).
   */
  public static KademliaId announceKademliaId(byte[] ohId, long timestampMs) {
    return KadContent.createKademliaId(timestampMs, deriveAnnounceNodeId(ohId).exportPublic());
  }

  /**
   * Builds the signed, fixed-size announce KadContent mapping {@code ohId} to {@code hostNodeId}.
   * Only the node id is stored (no endpoint): a sender resolves the host node's connection points
   * via the regular NodeInfo lookup, so a single record reveals as little as possible.
   */
  public static KadContent buildAnnounceContent(byte[] ohId, KademliaId hostNodeId, long nowMs) {
    OhNodeRecord.Builder record =
        OhNodeRecord.newBuilder()
            .setOhIdHash(ByteString.copyFrom(Sha256Hash.create(ohId).getBytes()))
            .setNodeId(ByteString.copyFrom(hostNodeId.getBytes()))
            .setAnnouncedAtMs(nowMs);

    byte[] padded = padToFixedSize(record);

    NodeId announceNodeId = deriveAnnounceNodeId(ohId);
    KadContent kadContent = new KadContent(nowMs, announceNodeId.exportPublic(), padded);
    kadContent.signWith(announceNodeId);
    return kadContent;
  }

  /** Pads the record with random bytes so the serialized size is exactly RECORD_SIZE_BYTES. */
  private static byte[] padToFixedSize(OhNodeRecord.Builder record) {
    int unpadded = record.clearPadding().build().getSerializedSize();
    // Search the padding length whose field overhead (tag + length varint) lands exactly on the
    // target; the varint length can shift by one byte around the 127-byte boundary.
    for (int padLen = Math.max(0, RECORD_SIZE_BYTES - unpadded - 3);
        padLen <= RECORD_SIZE_BYTES - unpadded;
        padLen++) {
      byte[] padding = new byte[padLen];
      SECURE_RANDOM.nextBytes(padding);
      byte[] serialized = record.setPadding(ByteString.copyFrom(padding)).build().toByteArray();
      if (serialized.length == RECORD_SIZE_BYTES) {
        return serialized;
      }
    }
    throw new IllegalStateException(
        "OhNodeRecord does not fit RECORD_SIZE_BYTES=" + RECORD_SIZE_BYTES);
  }

  /**
   * Picks the newest valid announce record for {@code ohId} from DHT search results. A result is
   * valid only if it is signed by the derived announce key (so peers cannot smuggle foreign content
   * into the answer — the pubkey check also pins the self-certifying Kademlia key, which is derived
   * from pubkey + record date), has exactly the fixed padded size (uniformity is part of the
   * anti-profiling contract), carries the matching oh_id_hash, and is not stale. Records up to
   * {@link #MAX_RECORD_AGE_MS} old are accepted deliberately: a record announced yesterday lives
   * under yesterday's rotated key and stays usable across the midnight-UTC boundary.
   *
   * @return the parsed record, or {@code null} if none qualifies
   */
  public static OhNodeRecord extractValidRecord(
      List<KadContent> contents, byte[] ohId, long nowMs) {
    if (contents == null || contents.isEmpty()) {
      return null;
    }
    byte[] announcePubkey = deriveAnnounceNodeId(ohId).exportPublic();
    byte[] expectedHash = Sha256Hash.create(ohId).getBytes();

    OhNodeRecord best = null;
    long bestTimestamp = Long.MIN_VALUE;
    for (KadContent content : contents) {
      if (content == null || content.getTimestamp() <= bestTimestamp) {
        continue;
      }
      if (nowMs - content.getTimestamp() > MAX_RECORD_AGE_MS) {
        continue;
      }
      if (content.getContent() == null || content.getContent().length != RECORD_SIZE_BYTES) {
        continue;
      }
      if (!Arrays.equals(content.getPubkey(), announcePubkey) || !content.verify()) {
        continue;
      }
      OhNodeRecord record;
      try {
        record = OhNodeRecord.parseFrom(content.getContent());
      } catch (InvalidProtocolBufferException e) {
        continue;
      }
      if (!Arrays.equals(record.getOhIdHash().toByteArray(), expectedHash)
          || record.getNodeId().size() != KademliaId.ID_LENGTH_BYTES) {
        continue;
      }
      best = record;
      bestTimestamp = content.getTimestamp();
    }
    return best;
  }
}
