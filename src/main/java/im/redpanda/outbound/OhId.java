package im.redpanda.outbound;

import com.google.protobuf.ByteString;
import im.redpanda.core.KademliaId;
import im.redpanda.crypt.Utils;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * The identifier of an Outbound Handle (OH) — the mailbox a light client rents on a node.
 *
 * <p>T113 (DDD review 2026-08-31, §2 ubiquitous language): before this type an oh_id travelled
 * through the code as a bare {@code byte[]} and, inside the stores, as a bare hex {@code String}.
 * Both are indistinguishable from every other byte array / string in scope (payloads, session tags,
 * node ids, replay nonces), the hex conversion was duplicated at every store entry point, and the
 * length rules lived in two unrelated places. This class is the single home for all of that.
 *
 * <h2>Length rules (the one place)</h2>
 *
 * <ul>
 *   <li><b>{@link #MIN_BYTES}..{@link #MAX_BYTES} (16..64)</b> — the general validity range. It is
 *       the range the outbound command surface (register / fetch / revoke / subscribe / ack_fetch)
 *       has enforced since MS02b: an oh_id is a client-chosen opaque secret, so the lower bound is
 *       what makes it unguessable and the upper bound is defense-in-depth against oversized fields.
 *       Every {@code OhId} instance satisfies it — there is no way to construct one that does not.
 *   <li><b>{@link #GARLIC_BYTES} (20)</b> — the length every oh_id on the <em>garlic</em> wire
 *       actually has, because those frames reuse the fixed-width Kademlia destination slot for it:
 *       {@code FlaschenpostPut.oh_id}, {@code CMD_DELIVER}/{@code CMD_DELIVER_TAGGED}/{@code
 *       CMD_DELIVER_ACKED} and {@code ReturnPath.ack_oh_id}. Those call sites check for exactly
 *       this length <em>in addition</em> to the general range, and reject anything else.
 * </ul>
 *
 * <p>The two rules overlap on purpose ({@code 16 <= 20 <= 64}): the same mailbox is addressable
 * both through the garlic path and through a direct outbound command. The consequence is the shared
 * namespace TD094 describes — a 20-byte garlic <em>node</em> destination is indistinguishable from
 * a 20-byte oh_id, which is what {@code MailboxDepositPolicy#tryDepositToLocalOh} exploits. This
 * type makes that sharing visible (a {@code KademliaId} does not silently become an {@code OhId}
 * any more; the conversion has to be written out) but does not yet remove it — that needs a wire
 * change.
 *
 * <p>Instances are immutable: the byte array is copied in and copied out, and the hex form (the key
 * used by the mailbox stores) is computed once.
 */
public final class OhId {

  /** Smallest accepted oh_id. Below this the identifier stops being unguessable. */
  public static final int MIN_BYTES = 16;

  /** Largest accepted oh_id (defense-in-depth against oversized wire fields). */
  public static final int MAX_BYTES = 64;

  /**
   * Length of every oh_id carried on the garlic wire — the frames reuse the fixed 20-byte Kademlia
   * destination slot. See the class comment and TD094.
   */
  public static final int GARLIC_BYTES = KademliaId.ID_LENGTH_BYTES;

  private final byte[] bytes;
  private final String hex;

  private OhId(byte[] bytes) {
    this.bytes = bytes;
    this.hex = Utils.bytesToHexString(bytes);
  }

  /**
   * @throws IllegalArgumentException if the length is outside {@link #MIN_BYTES}..{@link
   *     #MAX_BYTES}
   */
  public static OhId fromBytes(byte[] bytes) {
    OhId ohId = fromBytesOrNull(bytes);
    if (ohId == null) {
      throw new IllegalArgumentException(
          "invalid oh_id length: " + (bytes == null ? "null" : bytes.length));
    }
    return ohId;
  }

  /**
   * Lenient counterpart of {@link #fromBytes}: {@code null} instead of an exception, for the wire
   * paths that answer BAD_REQUEST on a malformed field rather than throwing.
   */
  public static OhId fromBytesOrNull(byte[] bytes) {
    if (bytes == null || isInvalidLength(bytes.length)) {
      return null;
    }
    return new OhId(bytes.clone());
  }

  /**
   * Wire boundary without the extra array copy of {@link #fromBytes}: the protobuf {@code bytes}
   * field is materialized exactly once.
   *
   * @throws IllegalArgumentException if the length is outside {@link #MIN_BYTES}..{@link
   *     #MAX_BYTES}
   */
  public static OhId fromByteString(ByteString bytes) {
    OhId ohId = fromByteStringOrNull(bytes);
    if (ohId == null) {
      throw new IllegalArgumentException(
          "invalid oh_id length: " + (bytes == null ? "null" : bytes.size()));
    }
    return ohId;
  }

  /** Wire boundary: protobuf {@code bytes} field to {@code OhId}, {@code null} if malformed. */
  public static OhId fromByteStringOrNull(ByteString bytes) {
    if (bytes == null || isInvalidLength(bytes.size())) {
      return null;
    }
    return new OhId(bytes.toByteArray());
  }

  /**
   * Reads a fixed-width {@link #GARLIC_BYTES}-byte oh_id from the buffer's current position,
   * consuming exactly that many bytes.
   *
   * @throws java.nio.BufferUnderflowException if fewer bytes remain
   */
  public static OhId readGarlicSlot(ByteBuffer buffer) {
    byte[] raw = new byte[GARLIC_BYTES];
    buffer.get(raw);
    return new OhId(raw);
  }

  /**
   * @throws IllegalArgumentException if the string is not valid hex of an accepted length
   */
  public static OhId fromHex(String hex) {
    if (hex == null || hex.length() % 2 != 0 || isInvalidLength(hex.length() / 2)) {
      throw new IllegalArgumentException(
          "invalid oh_id hex length: " + (hex == null ? "null" : hex.length()));
    }
    byte[] raw = new byte[hex.length() / 2];
    for (int i = 0; i < raw.length; i++) {
      int hi = Character.digit(hex.charAt(2 * i), 16);
      int lo = Character.digit(hex.charAt(2 * i + 1), 16);
      if (hi < 0 || lo < 0) {
        throw new IllegalArgumentException("invalid oh_id hex");
      }
      raw[i] = (byte) ((hi << 4) | lo);
    }
    return new OhId(raw);
  }

  private static boolean isInvalidLength(int length) {
    return length < MIN_BYTES || length > MAX_BYTES;
  }

  /** The raw bytes — a fresh copy, the instance stays immutable. */
  public byte[] toBytes() {
    return bytes.clone();
  }

  /** Appends the raw bytes to {@code buffer} without an intermediate copy (signing/serializing). */
  public void writeTo(ByteBuffer buffer) {
    buffer.put(bytes);
  }

  /** Wire boundary: {@code OhId} to a protobuf {@code bytes} field. */
  public ByteString toByteString() {
    return ByteString.copyFrom(bytes);
  }

  /**
   * Lowercase hex, byte-identical to {@code Utils.bytesToHexString} — this is the persisted key of
   * the mailbox stores, so the encoding is part of the storage format and must not change.
   */
  public String toHex() {
    return hex;
  }

  public int length() {
    return bytes.length;
  }

  /** {@code true} if this id has the fixed garlic-wire length, see {@link #GARLIC_BYTES}. */
  public boolean hasGarlicLength() {
    return bytes.length == GARLIC_BYTES;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof OhId that && Arrays.equals(bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  /**
   * Truncated on purpose: knowing an oh_id is the capability to deposit into that mailbox, so the
   * full value must not end up in a log line by accident. Call {@link #toHex()} where the complete
   * value is really wanted.
   */
  @Override
  public String toString() {
    return "OhId(" + hex.substring(0, 8) + "...)";
  }
}
