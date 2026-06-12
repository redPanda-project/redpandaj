package im.redpanda.core;

import im.redpanda.crypt.CryptoUtils;
import im.redpanda.crypt.Sha256Hash;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters;
import org.bouncycastle.crypto.params.X25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;
import org.bouncycastle.crypto.signers.Ed25519Signer;

/**
 * MS03 node identity: a dual keypair with strict key separation.
 *
 * <ul>
 *   <li><b>Signing</b>: Ed25519 — node identity, message/content signatures (64-byte deterministic
 *       signatures).
 *   <li><b>Encryption</b>: X25519 — Diffie-Hellman key exchange (garlic messages, TCP v23 session
 *       keys).
 * </ul>
 *
 * <p>The {@link KademliaId} is derived from the SHA-256 of the 32-byte Ed25519 verify key. HashCash
 * makes the computation of many valid identities expensive: the double SHA-256 of the verify key
 * must start with at least {@link #POW_MIN_LEADING_ZERO_BITS} zero bits.
 *
 * <p>Export formats (MS03 wire formats):
 *
 * <pre>
 * public  (64 bytes):  [32 verifyKey][32 encryptionPubKey]
 * private (128 bytes): [32 signingKey][32 verifyKey][32 encryptionKey][32 encryptionPubKey]
 * </pre>
 */
public class NodeId implements Serializable {

  /** Public export length: 32-byte Ed25519 verify key + 32-byte X25519 encryption key. */
  public static final int PUBLIC_KEYLEN = 64;

  /** Private export length: both private keys with their public counterparts. */
  public static final int PRIVATE_KEYLEN = 128;

  /** Ed25519 signatures are always 64 bytes (no DER encoding). */
  public static final int SIGNATURE_LEN = 64;

  /** Length of a single key component (Ed25519/X25519 keys are 32 bytes each). */
  public static final int KEY_COMPONENT_LEN = 32;

  /** HashCash: required leading zero bits of SHA256d(verifyKey) — Tier 0. */
  public static final int POW_MIN_LEADING_ZERO_BITS = 8;

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private transient Ed25519PrivateKeyParameters signingKey;
  private transient Ed25519PublicKeyParameters verifyKey;
  private transient X25519PrivateKeyParameters encryptionKey;
  private transient X25519PublicKeyParameters encryptionPubKey;

  KademliaId kademliaId;

  private NodeId(
      Ed25519PrivateKeyParameters signingKey,
      Ed25519PublicKeyParameters verifyKey,
      X25519PrivateKeyParameters encryptionKey,
      X25519PublicKeyParameters encryptionPubKey) {
    this.signingKey = signingKey;
    this.verifyKey = verifyKey;
    this.encryptionKey = encryptionKey;
    this.encryptionPubKey = encryptionPubKey;
  }

  /** Obtains a NodeId object from a given KademliaId, the keys are unknown. */
  public NodeId(KademliaId kademliaId) {
    this.kademliaId = kademliaId;
  }

  /**
   * Generates a new NodeId with a fresh random dual keypair satisfying the HashCash requirement
   * (skipped in unit tests for speed, as before).
   */
  public NodeId() {
    while (true) {
      NodeId candidate = generateWithSimpleKey();
      this.signingKey = candidate.signingKey;
      this.verifyKey = candidate.verifyKey;
      this.encryptionKey = candidate.encryptionKey;
      this.encryptionPubKey = candidate.encryptionPubKey;

      if (Log.isJUnitTest() || checkValid()) {
        break;
      }
    }
  }

  /** Generates a new NodeId with a new random key without the HashCash requirement. */
  public static NodeId generateWithSimpleKey() {
    Ed25519PrivateKeyParameters signing = new Ed25519PrivateKeyParameters(SECURE_RANDOM);
    X25519PrivateKeyParameters encryption = new X25519PrivateKeyParameters(SECURE_RANDOM);
    return new NodeId(
        signing, signing.generatePublicKey(), encryption, encryption.generatePublicKey());
  }

  /**
   * Deterministically derives a NodeId from a 32-byte seed: the Ed25519 signing key is the seed
   * itself, the X25519 encryption key is SHA-256 of the seed (key separation). Same seed → same
   * NodeId on every node. Used for the self-certifying OH announce records (see {@code OhDht}).
   */
  public static NodeId fromSeed(byte[] seed) {
    if (seed == null || seed.length != KEY_COMPONENT_LEN) {
      throw new IllegalArgumentException("seed must be exactly 32 bytes");
    }
    Ed25519PrivateKeyParameters signing = new Ed25519PrivateKeyParameters(seed, 0);
    X25519PrivateKeyParameters encryption =
        new X25519PrivateKeyParameters(Sha256Hash.create(seed).getBytes(), 0);
    return new NodeId(
        signing, signing.generatePublicKey(), encryption, encryption.generatePublicKey());
  }

  /**
   * Checks if this NodeId satisfies the HashCash requirement: the double SHA-256 of the 32-byte
   * verify key starts with at least {@link #POW_MIN_LEADING_ZERO_BITS} zero bits.
   */
  public boolean checkValid() {
    Sha256Hash sha256Hash = Sha256Hash.createDouble(verifyKey.getEncoded());
    return CryptoUtils.countLeadingZeroBits(sha256Hash.getBytes()) >= POW_MIN_LEADING_ZERO_BITS;
  }

  public boolean hasPrivate() {
    return signingKey != null;
  }

  public boolean hasKey() {
    return verifyKey != null;
  }

  /** The KademliaId is the SHA-256 of the 32-byte Ed25519 verify key (first 20 bytes). */
  public KademliaId getKademliaId() {
    if (kademliaId == null) {
      kademliaId = fromVerifyKey(verifyKey.getEncoded());
    }
    return kademliaId;
  }

  private static KademliaId fromVerifyKey(byte[] verifyKeyBytes) {
    return KademliaId.fromFirstBytes(Sha256Hash.create(verifyKeyBytes).getBytes());
  }

  /** Exports the public keys: {@code [32 verifyKey][32 encryptionPubKey]}. */
  public byte[] exportPublic() {
    ByteBuffer buffer = ByteBuffer.allocate(PUBLIC_KEYLEN);
    buffer.put(verifyKey.getEncoded());
    buffer.put(encryptionPubKey.getEncoded());
    return buffer.array();
  }

  /**
   * Exports both keypairs: {@code [32 signingKey][32 verifyKey][32 encryptionKey][32
   * encryptionPubKey]}.
   */
  public byte[] exportWithPrivate() {
    if (!hasPrivate()) {
      throw new IllegalStateException("this NodeId has no private keys to export!");
    }
    ByteBuffer buffer = ByteBuffer.allocate(PRIVATE_KEYLEN);
    buffer.put(signingKey.getEncoded());
    buffer.put(verifyKey.getEncoded());
    buffer.put(encryptionKey.getEncoded());
    buffer.put(encryptionPubKey.getEncoded());
    return buffer.array();
  }

  /** Imports a 64-byte public export. */
  public static NodeId importPublic(byte[] bytes) {
    if (bytes == null || bytes.length != PUBLIC_KEYLEN) {
      throw new IllegalArgumentException(
          "public NodeId export must be exactly " + PUBLIC_KEYLEN + " bytes");
    }
    Ed25519PublicKeyParameters verify = new Ed25519PublicKeyParameters(bytes, 0);
    X25519PublicKeyParameters encryption = new X25519PublicKeyParameters(bytes, KEY_COMPONENT_LEN);
    return new NodeId(null, verify, null, encryption);
  }

  /** Imports a 128-byte private export. */
  public static NodeId importWithPrivate(byte[] bytes) {
    if (bytes == null || bytes.length != PRIVATE_KEYLEN) {
      throw new IllegalArgumentException(
          "private NodeId export must be exactly " + PRIVATE_KEYLEN + " bytes");
    }
    Ed25519PrivateKeyParameters signing = new Ed25519PrivateKeyParameters(bytes, 0);
    X25519PrivateKeyParameters encryption =
        new X25519PrivateKeyParameters(bytes, 2 * KEY_COMPONENT_LEN);

    Ed25519PublicKeyParameters verify = signing.generatePublicKey();
    X25519PublicKeyParameters encryptionPub = encryption.generatePublicKey();

    if (!Arrays.equals(verify.getEncoded(), Arrays.copyOfRange(bytes, 32, 64))
        || !Arrays.equals(encryptionPub.getEncoded(), Arrays.copyOfRange(bytes, 96, 128))) {
      throw new IllegalArgumentException("public keys in private export do not match private keys");
    }
    return new NodeId(signing, verify, encryption, encryptionPub);
  }

  public static NodeId fromBufferGetPublic(ByteBuffer buffer) {
    byte[] publicKeyBytes = new byte[PUBLIC_KEYLEN];
    buffer.get(publicKeyBytes);
    return importPublic(publicKeyBytes);
  }

  /** Signs the bytes with Ed25519 — deterministic 64-byte signature (no hashing beforehand). */
  public byte[] sign(byte[] bytesToSign) {
    if (!hasPrivate()) {
      throw new RuntimeException(
          "this NodeId can not be used for signing since there is no private key!");
    }
    Ed25519Signer signer = new Ed25519Signer();
    signer.init(true, signingKey);
    signer.update(bytesToSign, 0, bytesToSign.length);
    return signer.generateSignature();
  }

  /** Verifies an Ed25519 signature (64 bytes) over the given bytes. */
  public boolean verify(byte[] bytesToVerify, byte[] signature) {
    if (verifyKey == null || signature == null || signature.length != SIGNATURE_LEN) {
      return false;
    }
    try {
      Ed25519Signer signer = new Ed25519Signer();
      signer.init(false, verifyKey);
      signer.update(bytesToVerify, 0, bytesToVerify.length);
      return signer.verifySignature(signature);
    } catch (RuntimeException e) {
      return false;
    }
  }

  /** The 32-byte Ed25519 verify key. */
  public byte[] getVerifyKeyBytes() {
    return verifyKey.getEncoded();
  }

  public X25519PublicKeyParameters getEncryptionPubKey() {
    return encryptionPubKey;
  }

  public X25519PrivateKeyParameters getEncryptionKey() {
    return encryptionKey;
  }

  /**
   * Sets the keys of this object from another NodeId if not already provided and checks that the
   * keys fit to the already provided KademliaId.
   */
  public void setKeys(NodeId source) throws KeypairDoesNotMatchException {
    if (this.verifyKey != null) {
      throw new RuntimeException("keys are already set for this NodeId!");
    }
    if (source == null || !source.hasKey()) {
      throw new RuntimeException("provided NodeId must contain keys when setting NodeId keys!");
    }
    if (kademliaId == null) {
      throw new RuntimeException("To check the keys there has to be already a known KademliaId!");
    }
    if (!source.getKademliaId().equals(this.kademliaId)) {
      throw new KeypairDoesNotMatchException();
    }
    this.signingKey = source.signingKey;
    this.verifyKey = source.verifyKey;
    this.encryptionKey = source.encryptionKey;
    this.encryptionPubKey = source.encryptionPubKey;
  }

  public static class KeypairDoesNotMatchException extends Exception {}

  /** Two NodeId are equal if their KademliaId is equal. */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof NodeId id) {
      return getKademliaId().equals(id.getKademliaId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getKademliaId().hashCode();
  }

  @Override
  public String toString() {
    return getKademliaId().toString();
  }

  private void readObject(ObjectInputStream aInputStream) throws IOException {
    boolean hasPrivate = aInputStream.readBoolean();
    byte[] bytes = new byte[hasPrivate ? PRIVATE_KEYLEN : PUBLIC_KEYLEN];
    aInputStream.readFully(bytes);
    try {
      NodeId nodeId = hasPrivate ? importWithPrivate(bytes) : importPublic(bytes);
      this.signingKey = nodeId.signingKey;
      this.verifyKey = nodeId.verifyKey;
      this.encryptionKey = nodeId.encryptionKey;
      this.encryptionPubKey = nodeId.encryptionPubKey;
    } catch (IllegalArgumentException e) {
      throw new IOException("could not deserialize NodeId", e);
    }
  }

  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    aOutputStream.writeBoolean(hasPrivate());
    if (hasPrivate()) {
      aOutputStream.write(exportWithPrivate());
    } else {
      aOutputStream.write(exportPublic());
    }
  }
}
