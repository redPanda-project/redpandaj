package im.redpanda.flaschenpost;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import javax.crypto.AEADBadTagException;
import org.junit.Test;

/** MS04: Flaschenpost v2 packet format — fixed 2048-byte size, layer crypto, validation. */
public class FlaschenpostV2Test {

  private static final SecureRandom RANDOM = new SecureRandom();

  @Test
  public void wireFormatConstants_areLocked() {
    // these constants are the wire format — changing them breaks frontend interop
    assertThat(FlaschenpostV2.PACKET_SIZE).isEqualTo(2048);
    assertThat(FlaschenpostV2.HEADER_LEN).isEqualTo(73);
    assertThat(FlaschenpostV2.BODY_HEADER_LEN).isEqualTo(48);
    assertThat(FlaschenpostV2.MAX_CIPHERTEXT_LEN).isEqualTo(1975);
    assertThat(FlaschenpostV2.MIN_CIPHERTEXT_LEN).isEqualTo(17);
    assertThat(FlaschenpostV2.HKDF_INFO)
        .isEqualTo("flaschenpost-v2".getBytes(StandardCharsets.US_ASCII));
  }

  @Test
  public void encryptBuildParseDecrypt_roundTrip() throws Exception {
    NodeId hop = NodeId.generateWithSimpleKey();
    byte[] plaintext = "layer plaintext".getBytes(StandardCharsets.UTF_8);

    byte[] body =
        FlaschenpostV2.encryptLayer(hop.getEncryptionPubKey(), hop.getKademliaId(), plaintext);
    int packetId = RANDOM.nextInt();
    byte[] packet = FlaschenpostV2.buildPacket(packetId, hop.getKademliaId(), body);

    assertThat(packet).hasSize(FlaschenpostV2.PACKET_SIZE);

    FlaschenpostV2 parsed = FlaschenpostV2.parse(packet);
    assertThat(parsed).isNotNull();
    assertThat(parsed.getPacketId()).isEqualTo(packetId);
    assertThat(parsed.getNextHop()).isEqualTo(hop.getKademliaId());
    assertThat(parsed.decryptLayer(hop.getEncryptionKey())).isEqualTo(plaintext);
  }

  @Test
  public void decryptLayer_withWrongKey_throwsAeadBadTag() throws Exception {
    NodeId hop = NodeId.generateWithSimpleKey();
    NodeId other = NodeId.generateWithSimpleKey();
    byte[] body =
        FlaschenpostV2.encryptLayer(hop.getEncryptionPubKey(), hop.getKademliaId(), new byte[32]);
    FlaschenpostV2 parsed =
        FlaschenpostV2.parse(FlaschenpostV2.buildPacket(1, hop.getKademliaId(), body));

    assertThatThrownBy(() -> parsed.decryptLayer(other.getEncryptionKey()))
        .isInstanceOf(AEADBadTagException.class);
  }

  @Test
  public void decryptLayer_withTamperedNextHop_throwsAeadBadTag() throws Exception {
    // the next_hop is the AAD: redirecting a packet to another relay must break authentication
    NodeId hop = NodeId.generateWithSimpleKey();
    byte[] body =
        FlaschenpostV2.encryptLayer(hop.getEncryptionPubKey(), hop.getKademliaId(), new byte[32]);
    KademliaId otherId = NodeId.generateWithSimpleKey().getKademliaId();
    FlaschenpostV2 redirected = FlaschenpostV2.parse(FlaschenpostV2.buildPacket(1, otherId, body));

    assertThatThrownBy(() -> redirected.decryptLayer(hop.getEncryptionKey()))
        .isInstanceOf(AEADBadTagException.class);
  }

  @Test
  public void parse_rejectsMalformedPackets() {
    NodeId hop = NodeId.generateWithSimpleKey();

    // wrong total size
    assertThat(FlaschenpostV2.parse(new byte[100])).isNull();
    assertThat(FlaschenpostV2.parse(new byte[FlaschenpostV2.PACKET_SIZE + 1])).isNull();
    assertThat(FlaschenpostV2.parse(null)).isNull();

    // wrong version byte
    byte[] packet = validPacket(hop);
    packet[0] = 0x01;
    assertThat(FlaschenpostV2.parse(packet)).isNull();

    // ciphertext length out of bounds
    packet = validPacket(hop);
    java.nio.ByteBuffer.wrap(packet).putInt(69, FlaschenpostV2.MAX_CIPHERTEXT_LEN + 1);
    assertThat(FlaschenpostV2.parse(packet)).isNull();
    java.nio.ByteBuffer.wrap(packet).putInt(69, FlaschenpostV2.MIN_CIPHERTEXT_LEN - 1);
    assertThat(FlaschenpostV2.parse(packet)).isNull();
  }

  @Test
  public void buildPacket_rejectsInvalidBodyLength() {
    KademliaId nextHop = NodeId.generateWithSimpleKey().getKademliaId();
    int maxBody = FlaschenpostV2.PACKET_SIZE - 1 - 4 - KademliaId.ID_LENGTH_BYTES;

    assertThatThrownBy(() -> FlaschenpostV2.buildPacket(1, nextHop, new byte[maxBody + 1]))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                FlaschenpostV2.buildPacket(
                    1,
                    nextHop,
                    new byte
                        [FlaschenpostV2.BODY_HEADER_LEN + FlaschenpostV2.MIN_CIPHERTEXT_LEN - 1]))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void buildPacket_withMaximumBody_isExactlyPacketSize() throws Exception {
    // a full-size layer: ciphertext fills the packet completely, zero padding
    NodeId hop = NodeId.generateWithSimpleKey();
    byte[] plaintext =
        new byte[FlaschenpostV2.MAX_CIPHERTEXT_LEN - 16]; // max ct minus GCM tag = 1959
    byte[] body =
        FlaschenpostV2.encryptLayer(hop.getEncryptionPubKey(), hop.getKademliaId(), plaintext);
    byte[] packet = FlaschenpostV2.buildPacket(7, hop.getKademliaId(), body);

    assertThat(packet).hasSize(FlaschenpostV2.PACKET_SIZE);
    FlaschenpostV2 parsed = FlaschenpostV2.parse(packet);
    assertThat(parsed).isNotNull();
    assertThat(parsed.decryptLayer(hop.getEncryptionKey())).isEqualTo(plaintext);
  }

  private static byte[] validPacket(NodeId hop) {
    try {
      byte[] body =
          FlaschenpostV2.encryptLayer(hop.getEncryptionPubKey(), hop.getKademliaId(), new byte[32]);
      return FlaschenpostV2.buildPacket(RANDOM.nextInt(), hop.getKademliaId(), body);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
