package im.redpanda.mailbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.crypt.Utils;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * T113: the length rules, the immutability and the hex encoding of {@link OhId} live in exactly one
 * place, so they are pinned in exactly one place.
 */
class OhIdTest {

  private static byte[] bytes(int length) {
    byte[] raw = new byte[length];
    for (int i = 0; i < length; i++) {
      raw[i] = (byte) (i + 1);
    }
    return raw;
  }

  // --- Length rules ---

  @Test
  void acceptsExactlyTwentyBytes() {
    assertThat(OhId.fromBytes(bytes(20)).length()).isEqualTo(20);
    assertThat(OhId.fromBytesOrNull(bytes(20))).isNotNull();
    assertThat(OhId.fromByteString(ByteString.copyFrom(bytes(20))).length()).isEqualTo(20);
  }

  @Test
  void rejectsEveryOtherLength() {
    // T129 (TD148): 16 and 64 were the bounds of the pre-T129 range the outbound commands accepted
    // although no garlic deposit could reach anything but a 20-byte mailbox.
    for (int length : new int[] {0, 1, 16, 19, 21, 32, 64, 1024}) {
      assertThatThrownBy(() -> OhId.fromBytes(bytes(length)))
          .as("length %s", length)
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(OhId.fromBytesOrNull(bytes(length))).as("length %s", length).isNull();
      assertThat(OhId.fromByteStringOrNull(ByteString.copyFrom(bytes(length))))
          .as("length %s", length)
          .isNull();
      assertThatThrownBy(() -> OhId.fromHex(Utils.bytesToHexString(bytes(length))))
          .as("length %s", length)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void rejectsNull() {
    assertThat(OhId.fromBytesOrNull(null)).isNull();
    assertThat(OhId.fromByteStringOrNull(null)).isNull();
    assertThatThrownBy(() -> OhId.fromBytes(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void garlicLengthIsTheKademliaIdWidth() {
    // The fixed 20-byte garlic destination slot is the oh_id width. If this ever stops holding,
    // the CMD_DELIVER paths and ReturnPath.ack_oh_id break.
    assertThat(OhId.GARLIC_BYTES).isEqualTo(KademliaId.ID_LENGTH_BYTES).isEqualTo(20);
  }

  // --- Hex round trip ---

  @Test
  void hexMatchesTheLegacyStoreKeyEncoding() {
    // The hex form is the persisted mailbox-store key; it must stay byte-identical to what the
    // pre-T113 stores wrote via Utils.bytesToHexString.
    byte[] raw = bytes(OhId.GARLIC_BYTES);
    assertThat(OhId.fromBytes(raw).toHex()).isEqualTo(Utils.bytesToHexString(raw));
  }

  @Test
  void hexRoundTrips() {
    OhId ohId = OhId.fromBytes(bytes(OhId.GARLIC_BYTES));
    assertThat(OhId.fromHex(ohId.toHex())).isEqualTo(ohId);
    assertThat(OhId.fromHex(ohId.toHex()).toBytes()).isEqualTo(ohId.toBytes());
  }

  @Test
  void hexWithLeadingZeroByteRoundTrips() {
    byte[] raw = bytes(OhId.GARLIC_BYTES);
    raw[0] = 0;
    OhId ohId = OhId.fromBytes(raw);
    assertThat(ohId.toHex()).startsWith("00");
    assertThat(OhId.fromHex(ohId.toHex())).isEqualTo(ohId);
  }

  @Test
  void rejectsMalformedHex() {
    assertThatThrownBy(() -> OhId.fromHex(null)).isInstanceOf(IllegalArgumentException.class);
    // odd number of characters
    assertThatThrownBy(() -> OhId.fromHex("0".repeat(41)))
        .isInstanceOf(IllegalArgumentException.class);
    // right length, non-hex characters
    assertThatThrownBy(() -> OhId.fromHex("zz" + "0".repeat(38)))
        .isInstanceOf(IllegalArgumentException.class);
    // valid hex, but too short / too long for an oh_id
    assertThatThrownBy(() -> OhId.fromHex("0".repeat(2 * (OhId.GARLIC_BYTES - 1))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> OhId.fromHex("0".repeat(2 * (OhId.GARLIC_BYTES + 1))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --- Equality ---

  @Test
  void equalsAndHashCodeAreValueBased() {
    OhId a = OhId.fromBytes(bytes(20));
    OhId same = OhId.fromBytes(bytes(20));
    byte[] otherRaw = bytes(20);
    otherRaw[0] ^= 0x01;
    OhId other = OhId.fromBytes(otherRaw);

    assertThat(a).isEqualTo(same).hasSameHashCodeAs(same);
    assertThat(a).isNotEqualTo(other);
    assertThat(a).isNotEqualTo(null);
    assertThat(a).isNotEqualTo(bytes(20));

    // usable as a map key (OutboundService's subscription registry relies on it)
    Map<OhId, String> map = new HashMap<>();
    map.put(a, "value");
    assertThat(map).containsEntry(same, "value");
    assertThat(map).doesNotContainKey(other);
  }

  @Test
  void differentPrefixSameLengthAreNotEqual() {
    byte[] raw = bytes(20);
    byte[] flipped = bytes(20);
    flipped[19] ^= 0x01;
    assertThat(OhId.fromBytes(raw)).isNotEqualTo(OhId.fromBytes(flipped));
  }

  // --- Immutability ---

  @Test
  void bytesAreCopiedIn() {
    byte[] raw = bytes(20);
    OhId ohId = OhId.fromBytes(raw);
    String hexBefore = ohId.toHex();

    raw[0] ^= (byte) 0xFF;

    assertThat(ohId.toHex()).isEqualTo(hexBefore);
    assertThat(ohId.toBytes()[0]).isEqualTo(bytes(20)[0]);
  }

  @Test
  void bytesAreCopiedOut() {
    OhId ohId = OhId.fromBytes(bytes(20));
    byte[] first = ohId.toBytes();
    first[0] ^= (byte) 0xFF;

    assertThat(ohId.toBytes()).isEqualTo(bytes(20));
    assertThat(ohId.toBytes()).isNotSameAs(first);
  }

  // --- Wire boundary ---

  @Test
  void byteStringRoundTrips() {
    OhId ohId = OhId.fromBytes(bytes(20));
    assertThat(OhId.fromByteStringOrNull(ohId.toByteString())).isEqualTo(ohId);
    assertThat(ohId.toByteString()).isEqualTo(ByteString.copyFrom(bytes(20)));
    assertThat(OhId.fromByteStringOrNull(ByteString.copyFrom(bytes(5)))).isNull();

    assertThat(OhId.fromByteString(ohId.toByteString())).isEqualTo(ohId);
    assertThatThrownBy(() -> OhId.fromByteString(ByteString.copyFrom(bytes(5))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> OhId.fromByteString(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void garlicSlotIsReadAndWrittenAtTheSameWidth() {
    OhId ohId = OhId.fromBytes(bytes(OhId.GARLIC_BYTES));

    ByteBuffer out = ByteBuffer.allocate(OhId.GARLIC_BYTES + 4);
    ohId.writeTo(out);
    out.putInt(42);
    out.flip();

    assertThat(OhId.readGarlicSlot(out)).isEqualTo(ohId);
    assertThat(out.getInt()).isEqualTo(42);
  }

  @Test
  void toStringDoesNotLeakTheWholeCapability() {
    OhId ohId = OhId.fromBytes(bytes(20));
    assertThat(ohId.toString()).doesNotContain(ohId.toHex());
    assertThat(ohId.toString()).contains(ohId.toHex().substring(0, 8));
  }
}
