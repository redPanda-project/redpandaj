package im.redpanda.flaschenpost;

import im.redpanda.core.KademliaId;
import im.redpanda.crypt.CryptoUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import org.bouncycastle.crypto.params.X25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;

/**
 * Flaschenpost v2 (MS04): fixed-size multi-hop garlic packet.
 *
 * <p>Wire format — every packet is exactly {@link #PACKET_SIZE} (2048) bytes:
 *
 * <pre>
 * [1  version = 0x02]
 * [4  packet_id]          // random int, dedup + loop protection
 * [20 next_hop]           // KademliaId of the relay that can peel this layer
 * [12 nonce]              // AES-256-GCM nonce
 * [32 ephemeral_pub]      // X25519 ephemeral public key for this layer
 * [4  ciphertext_len]
 * [N  ciphertext + 16-byte GCM tag]
 * [P  random padding]     // fill to 2048 bytes total
 * </pre>
 *
 * <p>Key derivation: {@code key = HKDF-SHA256(ikm = X25519(ephemeralPriv, hopEncPub), salt =
 * ephemeralPub, info = "flaschenpost-v2")}. The AAD is the 20-byte {@code next_hop} KademliaId,
 * binding each layer to the relay that is meant to peel it. A relay that cannot authenticate the
 * layer drops the packet silently.
 *
 * <p>Layer plaintexts start with a command byte:
 *
 * <pre>
 * CMD_FORWARD        (0x01): [1 cmd][20 inner next_hop][12 nonce][32 ephemeral_pub][4 ct_len][ct + tag]
 * CMD_DELIVER        (0x02): [1 cmd][20 oh_id][4 payload_len][payload][optional padding]
 * CMD_DELIVER_TAGGED (0x03): [1 cmd][20 oh_id][16 session_tag][4 payload_len][payload][optional padding]
 * CMD_DELIVER_ACKED  (0x04): [1 cmd][20 oh_id][1 tag_len (0|16)][tag_len session_tag]
 *                            [return_path][4 payload_len][payload][optional padding]
 * </pre>
 *
 * <p>{@code CMD_DELIVER_TAGGED} (MS05) is the reverse-garlic deliver: identical to {@code
 * CMD_DELIVER} plus a 16-byte session tag that is stored on the mailbox {@code MailItem}, so the
 * fetching client can correlate the reply with a conversation. The tag stays inside the innermost
 * encrypted layer — relays never see it.
 *
 * <p>{@code CMD_DELIVER_ACKED} (MS06) is a deliver that requests an R-ACK: it carries an optional
 * session tag (length-prefixed, so tagged replies and untagged forward messages share one command)
 * followed by a {@link ReturnPath} block. The node that makes the final deposit decision builds a
 * {@code RoutingAck} and sends it back through the return-path hops as a standard MS04 onion (see
 * {@link RoutingAckSender}).
 *
 * <p>After peeling a {@code CMD_FORWARD} layer, the relay rebuilds a fresh 2048-byte packet with a
 * new random {@code packet_id}, the inner {@code next_hop} and the remaining plaintext as body,
 * padded with random bytes (see {@link #buildPacket}).
 */
public final class FlaschenpostV2 {

  /** Version byte of the fixed-size garlic packet format. */
  public static final byte VERSION = 0x02;

  /** Every Flaschenpost v2 packet is exactly this many bytes. */
  public static final int PACKET_SIZE = 2048;

  /** HKDF info string for the per-layer key derivation (domain-separated from "garlic-v2"). */
  public static final byte[] HKDF_INFO = "flaschenpost-v2".getBytes(StandardCharsets.US_ASCII);

  /** Layer command: peel and forward the inner packet to the contained next hop. */
  public static final byte CMD_FORWARD = 0x01;

  /** Layer command: final hop — deposit the payload into the OH mailbox. */
  public static final byte CMD_DELIVER = 0x02;

  /**
   * Layer command (MS05, reverse garlic): final hop — deposit the payload into the OH mailbox
   * together with the 16-byte session tag that follows the oh_id.
   */
  public static final byte CMD_DELIVER_TAGGED = 0x03;

  /**
   * Layer command (MS06, R-ACK): final hop — deposit the payload like {@link #CMD_DELIVER} / {@link
   * #CMD_DELIVER_TAGGED} and send a {@code RoutingAck} back via the contained {@link ReturnPath}.
   */
  public static final byte CMD_DELIVER_ACKED = 0x04;

  /** Length of the reverse-garlic session tag inside a {@link #CMD_DELIVER_TAGGED} layer. */
  public static final int SESSION_TAG_LEN = 16;

  /** Length of a layer body prefix: [12 nonce][32 ephemeral_pub][4 ciphertext_len] (48). */
  public static final int BODY_HEADER_LEN =
      CryptoUtils.GCM_NONCE_LEN + CryptoUtils.X25519_KEY_LEN + 4;

  /** Header length before the ciphertext: version + packet_id + next_hop + body header (73). */
  public static final int HEADER_LEN = 1 + 4 + KademliaId.ID_LENGTH_BYTES + BODY_HEADER_LEN;

  /** Maximum ciphertext length (including tag) that fits into a packet (1975). */
  public static final int MAX_CIPHERTEXT_LEN = PACKET_SIZE - HEADER_LEN;

  /** Minimum ciphertext length: GCM tag + at least the 1-byte layer command (17). */
  public static final int MIN_CIPHERTEXT_LEN = CryptoUtils.GCM_TAG_LEN + 1;

  private static final SecureRandom RANDOM = new SecureRandom();

  private final int packetId;
  private final KademliaId nextHop;
  private final byte[] nonce;
  private final byte[] ephemeralPub;
  private final byte[] ciphertext;

  private FlaschenpostV2(
      int packetId, KademliaId nextHop, byte[] nonce, byte[] ephemeralPub, byte[] ciphertext) {
    this.packetId = packetId;
    this.nextHop = nextHop;
    this.nonce = nonce;
    this.ephemeralPub = ephemeralPub;
    this.ciphertext = ciphertext;
  }

  /**
   * Parses a Flaschenpost v2 packet.
   *
   * @return the parsed packet or {@code null} if the packet is malformed (wrong size, wrong
   *     version, ciphertext length out of bounds)
   */
  public static FlaschenpostV2 parse(byte[] packet) {
    if (packet == null || packet.length != PACKET_SIZE) {
      return null;
    }
    ByteBuffer buffer = ByteBuffer.wrap(packet);
    if (buffer.get() != VERSION) {
      return null;
    }
    int packetId = buffer.getInt();
    KademliaId nextHop = KademliaId.fromBuffer(buffer);
    byte[] nonce = new byte[CryptoUtils.GCM_NONCE_LEN];
    buffer.get(nonce);
    byte[] ephemeralPub = new byte[CryptoUtils.X25519_KEY_LEN];
    buffer.get(ephemeralPub);
    int ciphertextLen = buffer.getInt();
    if (ciphertextLen < MIN_CIPHERTEXT_LEN || ciphertextLen > MAX_CIPHERTEXT_LEN) {
      return null;
    }
    byte[] ciphertext = new byte[ciphertextLen];
    buffer.get(ciphertext);
    // remaining bytes are padding and are ignored
    return new FlaschenpostV2(packetId, nextHop, nonce, ephemeralPub, ciphertext);
  }

  /**
   * Assembles a 2048-byte packet from a layer body ({@code [nonce][ephemeral_pub][ct_len][ct]} as
   * produced by {@link #encryptLayer} or carried inside a peeled {@code CMD_FORWARD} plaintext) and
   * fills the rest with random padding.
   *
   * @throws IllegalArgumentException if the body does not fit or is too short
   */
  public static byte[] buildPacket(int packetId, KademliaId nextHop, byte[] body) {
    int minBodyLen = BODY_HEADER_LEN + MIN_CIPHERTEXT_LEN;
    int maxBodyLen = PACKET_SIZE - 1 - 4 - KademliaId.ID_LENGTH_BYTES;
    if (body.length < minBodyLen || body.length > maxBodyLen) {
      throw new IllegalArgumentException("invalid flaschenpost v2 body length: " + body.length);
    }
    ByteBuffer buffer = ByteBuffer.allocate(PACKET_SIZE);
    buffer.put(VERSION);
    buffer.putInt(packetId);
    buffer.put(nextHop.getBytes());
    buffer.put(body);
    byte[] padding = new byte[buffer.remaining()];
    RANDOM.nextBytes(padding);
    buffer.put(padding);
    return buffer.array();
  }

  /**
   * Encrypts one garlic layer for the given hop: generates a fresh ephemeral X25519 key and a
   * random nonce, derives the AES-256-GCM key via HKDF and authenticates the hop's KademliaId as
   * AAD. Reference implementation for the client-side layer construction.
   *
   * @return the layer body {@code [12 nonce][32 ephemeral_pub][4 ct_len][ciphertext + tag]}
   */
  public static byte[] encryptLayer(
      X25519PublicKeyParameters hopEncryptionKey, KademliaId hopId, byte[] plaintext)
      throws GeneralSecurityException {
    byte[] nonce = new byte[CryptoUtils.GCM_NONCE_LEN];
    RANDOM.nextBytes(nonce);
    X25519PrivateKeyParameters ephemeralKey = new X25519PrivateKeyParameters(RANDOM);
    byte[] ephemeralPub = ephemeralKey.generatePublicKey().getEncoded();

    byte[] key = deriveKey(CryptoUtils.x25519(ephemeralKey, hopEncryptionKey), ephemeralPub);
    byte[] ciphertext = CryptoUtils.encryptGcm(key, nonce, plaintext, hopId.getBytes());

    ByteBuffer body = ByteBuffer.allocate(BODY_HEADER_LEN + ciphertext.length);
    body.put(nonce);
    body.put(ephemeralPub);
    body.putInt(ciphertext.length);
    body.put(ciphertext);
    return body.array();
  }

  /**
   * Decrypts this packet's layer with our X25519 encryption key. The AAD is the packet's {@code
   * next_hop}, so this only succeeds on the relay the layer was encrypted for.
   *
   * @throws javax.crypto.AEADBadTagException if the layer is not encrypted to us or was tampered
   *     with — the caller must drop the packet silently
   */
  public byte[] decryptLayer(X25519PrivateKeyParameters encryptionKey)
      throws GeneralSecurityException {
    byte[] key =
        deriveKey(
            CryptoUtils.x25519(encryptionKey, new X25519PublicKeyParameters(ephemeralPub, 0)),
            ephemeralPub);
    return CryptoUtils.decryptGcm(key, nonce, ciphertext, nextHop.getBytes());
  }

  private static byte[] deriveKey(byte[] sharedSecret, byte[] ephemeralPublicKey) {
    return CryptoUtils.hkdfSha256(
        sharedSecret, ephemeralPublicKey, HKDF_INFO, CryptoUtils.AES_KEY_LEN);
  }

  public int getPacketId() {
    return packetId;
  }

  public KademliaId getNextHop() {
    return nextHop;
  }
}
