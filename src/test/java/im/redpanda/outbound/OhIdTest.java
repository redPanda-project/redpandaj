package im.redpanda.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import im.redpanda.core.KademliaId;
import im.redpanda.crypt.Utils;
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
  void acceptsTheWholeDocumentedRange() {
    for (int length = OhId.MIN_BYTES; length <= OhId.MAX_BYTES; length++) {
      assertThat(OhId.fromBytes(bytes(length)).length()).isEqualTo(length);
    }
  }

  @Test
  void rejectsLengthsOutsideTheRange() {
    for (int length : new int[] {0, 1, OhId.MIN_BYTES - 1, OhId.MAX_BYTES + 1, 1024}) {
      assertThatThrownBy(() -> OhId.fromBytes(bytes(length)))
          .as("length %s", length)
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(OhId.fromBytesOrNull(bytes(length))).as("length %s", length).isNull();
    }
  }

  @Test
  void rejectsNull() {
    assertThat(OhId.fromBytesOrNull(null)).isNull();
    assertThat(OhId.fromByteStringOrNull(null)).isNull();
    assertThatThrownBy(() -> OhId.fromBytes(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void garlicLengthIsTheKademliaIdWidthAndInsideTheGeneralRange() {
    // The shared namespace TD094 describes: the fixed 20-byte garlic destination slot is a valid
    // oh_id. If this ever stops holding, tryDepositToLocalOh and the CMD_DELIVER paths break.
    assertThat(OhId.GARLIC_BYTES).isEqualTo(KademliaId.ID_LENGTH_BYTES);
    assertThat(OhId.GARLIC_BYTES).isBetween(OhId.MIN_BYTES, OhId.MAX_BYTES);
    assertThat(OhId.fromBytes(bytes(OhId.GARLIC_BYTES)).hasGarlicLength()).isTrue();
    assertThat(OhId.fromBytes(bytes(OhId.MAX_BYTES)).hasGarlicLength()).isFalse();
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
    OhId ohId = OhId.fromBytes(bytes(24));
    assertThat(OhId.fromHex(ohId.toHex())).isEqualTo(ohId);
    assertThat(OhId.fromHex(ohId.toHex()).toBytes()).isEqualTo(ohId.toBytes());
  }

  @Test
  void hexWithLeadingZeroByteRoundTrips() {
    byte[] raw = bytes(OhId.MIN_BYTES);
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
    assertThatThrownBy(() -> OhId.fromHex("0".repeat(2 * (OhId.MIN_BYTES - 1))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> OhId.fromHex("0".repeat(2 * (OhId.MAX_BYTES + 1))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --- Equality ---

  @Test
  void equalsAndHashCodeAreValueBased() {
    OhId a = OhId.fromBytes(bytes(20));
    OhId same = OhId.fromBytes(bytes(20));
    OhId other = OhId.fromBytes(bytes(21));

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
