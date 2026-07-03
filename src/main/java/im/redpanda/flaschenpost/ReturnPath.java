package im.redpanda.flaschenpost;

import im.redpanda.core.KademliaId;
import im.redpanda.crypt.CryptoUtils;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * MS06 return-path block: tells the depositing node where to send the {@code RoutingAck} (R-ACK).
 *
 * <p>Wire format (plaintext inside the innermost {@code CMD_DELIVER_ACKED} layer, or the {@code
 * FlaschenpostPut.return_path} field on the MS02b fallback):
 *
 * <pre>
 * [20 ack_oh_id]         // sender's OH mailbox the R-ACK is deposited into
 * [16 ack_session_tag]   // sender-chosen tag correlating the R-ACK with the message
 * [1  hop_count]         // 0..MAX_HOPS return-path relays
 * hop_count x [20 kademlia_id][32 encryption_pub]
 * </pre>
 *
 * <p>Like the MS05 RGB (master-spec Decision 6), the block carries <em>hop descriptors</em> rather
 * than pre-encrypted onion layers — stateless GCM relays cannot transport a payload through
 * pre-built layers, so the R-ACK sender builds a standard MS04 onion over these hops itself. With
 * {@code hop_count = 0} the depositing node delivers the R-ACK directly (local deposit or MS02b
 * forward toward the ack OH host).
 */
public record ReturnPath(byte[] ackOhId, byte[] ackSessionTag, List<Hop> hops) {

  /** One return-path relay: its KademliaId and X25519 encryption public key. */
  public record Hop(KademliaId kademliaId, byte[] encryptionPub) {}

  /** Maximum number of return-path hops a sender may request. */
  public static final int MAX_HOPS = 4;

  /** Serialized size of one hop descriptor (20-byte KademliaId + 32-byte X25519 key). */
  public static final int HOP_LEN = KademliaId.ID_LENGTH_BYTES + CryptoUtils.X25519_KEY_LEN;

  /** Serialized size of the fixed part: ack_oh_id + ack_session_tag + hop_count byte (37). */
  public static final int FIXED_LEN =
      KademliaId.ID_LENGTH_BYTES + FlaschenpostV2.SESSION_TAG_LEN + 1;

  /** Maximum serialized size of a return-path block (245 bytes at {@link #MAX_HOPS}). */
  public static final int MAX_SERIALIZED_LEN = FIXED_LEN + MAX_HOPS * HOP_LEN;

  /**
   * Parses a return-path block from the buffer's current position, consuming exactly the block.
   *
   * @return the parsed block or {@code null} if it is malformed (truncated, hop_count out of
   *     bounds) — callers must drop the packet / reject the deposit
   */
  public static ReturnPath parse(ByteBuffer buffer) {
    if (buffer.remaining() < FIXED_LEN) {
      return null;
    }
    byte[] ackOhId = new byte[KademliaId.ID_LENGTH_BYTES];
    buffer.get(ackOhId);
    byte[] ackSessionTag = new byte[FlaschenpostV2.SESSION_TAG_LEN];
    buffer.get(ackSessionTag);
    int hopCount = buffer.get() & 0xFF;
    if (hopCount > MAX_HOPS || buffer.remaining() < hopCount * HOP_LEN) {
      return null;
    }
    Hop[] hops = new Hop[hopCount];
    for (int i = 0; i < hopCount; i++) {
      KademliaId kademliaId = KademliaId.fromBuffer(buffer);
      byte[] encryptionPub = new byte[CryptoUtils.X25519_KEY_LEN];
      buffer.get(encryptionPub);
      hops[i] = new Hop(kademliaId, encryptionPub);
    }
    return new ReturnPath(ackOhId, ackSessionTag, List.of(hops));
  }

  /**
   * Parses a standalone serialized block (e.g. {@code FlaschenpostPut.return_path}). Unlike {@link
   * #parse(ByteBuffer)} trailing bytes are rejected — the field must contain exactly one block.
   */
  public static ReturnPath parseExact(byte[] serialized) {
    ByteBuffer buffer = ByteBuffer.wrap(serialized);
    ReturnPath returnPath = parse(buffer);
    return returnPath != null && !buffer.hasRemaining() ? returnPath : null;
  }

  /** Serializes this block into the documented wire format. */
  public byte[] serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(FIXED_LEN + hops.size() * HOP_LEN);
    buffer.put(ackOhId);
    buffer.put(ackSessionTag);
    buffer.put((byte) hops.size());
    for (Hop hop : hops) {
      buffer.put(hop.kademliaId().getBytes());
      buffer.put(hop.encryptionPub());
    }
    return buffer.array();
  }
}
