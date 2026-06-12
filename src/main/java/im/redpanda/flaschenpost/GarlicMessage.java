package im.redpanda.flaschenpost;

import im.redpanda.core.*;
import im.redpanda.crypt.CryptoUtils;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.crypto.AEADBadTagException;
import org.bouncycastle.crypto.params.X25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;

/**
 * Garlic message v2 (MS03): authenticated encryption with AES-256-GCM, ephemeral X25519 key
 * exchange and HKDF-SHA256 key derivation.
 *
 * <p>Wire format:
 *
 * <pre>
 * [1  version/GMType = 0x02]
 * [4  totalLen (bytes after this field)]
 * [20 destination KademliaId]
 * [12 nonce (random)]
 * [32 ephemeral X25519 public key]
 * [4  ciphertextLen]
 * [N  ciphertext + 16-byte GCM tag]
 * </pre>
 *
 * <p>Key derivation: {@code key = HKDF-SHA256(ikm = X25519(ephemeralPriv, targetEncPub), salt =
 * ephemeralPub, info = "garlic-v2")}. The AAD is the 20-byte destination KademliaId, binding the
 * ciphertext to its intended recipient. There is no separate signature anymore — the GCM tag
 * authenticates the message; a tampered ciphertext fails with {@link AEADBadTagException} and the
 * packet is dropped.
 */
public class GarlicMessage extends Flaschenpost {

  public static final byte VERSION = 0x02;
  public static final byte[] HKDF_INFO = "garlic-v2".getBytes();
  public static final int NONCE_LEN = CryptoUtils.GCM_NONCE_LEN;
  public static final int EPHEMERAL_KEY_LEN = CryptoUtils.X25519_KEY_LEN;

  private static final SecureRandom RANDOM = new SecureRandom();

  /**
   * This is the {@link NodeId} containing the public keys of the target {@link
   * im.redpanda.core.Peer}/{@link im.redpanda.core.Node} and is only used for the creation process.
   */
  private NodeId targetsNodeId;

  /**
   * The ephemeral X25519 key used for encryption. It is never reused: there is always a new
   * ephemeral key for every new garlic message.
   */
  private X25519PrivateKeyParameters ephemeralKey;

  /** The 32-byte public part of the ephemeral key (sent on the wire). */
  private byte[] ephemeralPublicKey;

  /** 12-byte random GCM nonce. */
  private byte[] nonce;

  /** The Content of the GarlicMessage is a List of other GMContent objects. */
  private final ArrayList<GMContent> nestedMessages;

  /** ciphertext including the 16-byte GCM tag */
  private byte[] encryptedInformation;

  public GarlicMessage(ServerContext serverContext, NodeId targetsNodeId) {
    super(serverContext, targetsNodeId.getKademliaId());

    this.targetsNodeId = targetsNodeId;
    this.nestedMessages = new ArrayList<>();
    this.nonce = new byte[NONCE_LEN];
    RANDOM.nextBytes(this.nonce);
    this.ephemeralKey = new X25519PrivateKeyParameters(RANDOM);
    this.ephemeralPublicKey = ephemeralKey.generatePublicKey().getEncoded();
  }

  public GarlicMessage(ServerContext serverContext, byte[] bytes) {
    super(serverContext);

    setContent(bytes);

    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    byte version = buffer.get();
    if (version != VERSION) {
      throw new RuntimeException("Unsupported garlic message version: " + version);
    }

    int overallByteLen = buffer.getInt();

    if (overallByteLen != buffer.remaining()) {
      throw new RuntimeException(
          "Warning, length of gm content wrong: " + overallByteLen + " " + buffer.remaining());
    }

    destination = KademliaId.fromBuffer(buffer);

    nonce = new byte[NONCE_LEN];
    buffer.get(nonce);

    ephemeralPublicKey = new byte[EPHEMERAL_KEY_LEN];
    buffer.get(ephemeralPublicKey);

    int encryptedLength = buffer.getInt();
    if (encryptedLength < CryptoUtils.GCM_TAG_LEN || encryptedLength > buffer.remaining()) {
      throw new RuntimeException("invalid garlic message ciphertext length: " + encryptedLength);
    }
    encryptedInformation = new byte[encryptedLength];
    buffer.get(encryptedInformation);

    nestedMessages = new ArrayList<>();
  }

  public void addGMContent(GMContent gmContent) {
    nestedMessages.add(gmContent);
  }

  public List<GMContent> getGMContent() {
    return nestedMessages;
  }

  @Override
  protected void computeContent() {

    int bytesForContent = 0;
    for (GMContent c : nestedMessages) {
      bytesForContent += 4;
      bytesForContent += c.getContent().length;
    }

    ByteBuffer contentToEncrypt = ByteBuffer.allocate(4 + bytesForContent);
    contentToEncrypt.putInt(nestedMessages.size());
    for (GMContent c : nestedMessages) {
      byte[] content = c.getContent();
      contentToEncrypt.putInt(content.length);
      contentToEncrypt.put(content);
    }

    if (contentToEncrypt.position() != contentToEncrypt.limit()) {
      throw new RuntimeException(
          "contentToEncrypt has wrong size: "
              + contentToEncrypt.position()
              + " "
              + contentToEncrypt.limit());
    }

    try {
      byte[] key =
          deriveKey(
              CryptoUtils.x25519(ephemeralKey, targetsNodeId.getEncryptionPubKey()),
              ephemeralPublicKey);

      byte[] encryptedBytes =
          CryptoUtils.encryptGcm(key, nonce, contentToEncrypt.array(), destination.getBytes());

      int overallLength =
          1
              + 4
              + KademliaId.ID_LENGTH_BYTES
              + NONCE_LEN
              + EPHEMERAL_KEY_LEN
              + 4
              + encryptedBytes.length;

      ByteBuffer encryptedAndAuthenticatedBytes = ByteBuffer.allocate(overallLength);
      encryptedAndAuthenticatedBytes.put(getGMType().getId());
      encryptedAndAuthenticatedBytes.putInt(overallLength - 1 - 4);
      encryptedAndAuthenticatedBytes.put(destination.getBytes());
      encryptedAndAuthenticatedBytes.put(nonce);
      encryptedAndAuthenticatedBytes.put(ephemeralPublicKey);
      encryptedAndAuthenticatedBytes.putInt(encryptedBytes.length);
      encryptedAndAuthenticatedBytes.put(encryptedBytes);

      if (encryptedAndAuthenticatedBytes.position() != encryptedAndAuthenticatedBytes.limit()) {
        throw new RuntimeException(
            "garlic message content has wrong size: "
                + encryptedAndAuthenticatedBytes.position()
                + " "
                + encryptedAndAuthenticatedBytes.limit());
      }

      this.encryptedInformation = encryptedBytes;
      setContent(encryptedAndAuthenticatedBytes.array());

    } catch (GeneralSecurityException e) {
      Log.sentry(e);
      e.printStackTrace();
    }
  }

  private static byte[] deriveKey(byte[] sharedSecret, byte[] ephemeralPublicKey) {
    return CryptoUtils.hkdfSha256(
        sharedSecret, ephemeralPublicKey, HKDF_INFO, CryptoUtils.AES_KEY_LEN);
  }

  protected void tryParseContent() {
    if (isTargetedToUs()) {
      parseContent();
    }
  }

  protected void parseContent() {

    if (!isTargetedToUs()) {
      throw new RuntimeException(
          "We can not decrypt this garlic message since it is not targeted to us!");
    }

    try {
      byte[] decryptedBytes = decryptPayload();

      ByteBuffer decryptedBuffer = ByteBuffer.wrap(decryptedBytes);
      int numberOfNeastedMessages = decryptedBuffer.getInt();

      for (int i = 0; i < numberOfNeastedMessages; i++) {

        int toParseBytes = decryptedBuffer.getInt();
        int startingPosition = decryptedBuffer.position();

        byte[] bytesForSingleGM = new byte[toParseBytes];

        decryptedBuffer.get(bytesForSingleGM);

        GMContent parsed = GMParser.parse(serverContext, bytesForSingleGM);
        addGMContent(parsed);

        if (decryptedBuffer.position() != startingPosition + toParseBytes) {
          throw new RuntimeException(
              "nested messages of garlic message could not be parsed correctly...");
        }
      }

    } catch (AEADBadTagException e) {
      // authentication failed: tampered ciphertext, wrong recipient binding (AAD) or wrong key —
      // drop the packet without parsing anything
      Log.put("garlic message authentication failed, dropping packet...", 50);
    } catch (GeneralSecurityException e) {
      Log.sentry(e);
      e.printStackTrace();
    }
  }

  /**
   * Decrypts the GCM payload with our own X25519 encryption key.
   *
   * @throws AEADBadTagException if the ciphertext was tampered with or not targeted to us
   */
  byte[] decryptPayload() throws GeneralSecurityException {
    byte[] key =
        deriveKey(
            CryptoUtils.x25519(
                serverContext.getNodeId().getEncryptionKey(),
                new X25519PublicKeyParameters(ephemeralPublicKey, 0)),
            ephemeralPublicKey);

    return CryptoUtils.decryptGcm(key, nonce, encryptedInformation, destination.getBytes());
  }

  @Override
  public GMType getGMType() {
    return GMType.GARLIC_MESSAGE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GarlicMessage that = (GarlicMessage) o;

    if (!Arrays.equals(ephemeralPublicKey, that.ephemeralPublicKey)) {
      return false;
    }
    if (!Arrays.equals(nonce, that.nonce)) {
      return false;
    }
    return Arrays.equals(encryptedInformation, that.encryptedInformation);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(ephemeralPublicKey);
    result = 31 * result + Arrays.hashCode(nonce);
    result = 31 * result + Arrays.hashCode(encryptedInformation);
    return result;
  }
}
