package im.redpanda.crypt.legacy;

import im.redpanda.core.KademliaId;
import im.redpanda.crypt.Sha256Hash;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;

/**
 * Pre-MS03 node identity: a single brainpoolp256r1 ECDH keypair used for both signing
 * (SHA256withECDSA) and encryption (ECDH + AES-CTR). The KademliaId is the SHA-256 of the 65-byte
 * public key export.
 *
 * <p>This class only exists to serve the <b>v22 transition path</b>: the protocol-v22 TCP handshake
 * for light clients that have not migrated to MS03 crypto yet, and the legacy ECDSA verification of
 * OH-auth signatures from those clients. It must not be used anywhere else.
 *
 * @deprecated for removal together with protocol-v22 support once all light clients have migrated
 *     to Ed25519/X25519 (frontend MS03).
 */
@Deprecated(forRemoval = true)
public class LegacyNodeId implements Serializable {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  public static final int PUBLIC_KEYLEN_LONG = 92;
  public static final int PUBLIC_KEYLEN = 65;
  public static final int PRIVATE_KEYLEN = 252;
  private static byte[] curveParametersASN1;

  private KeyPair keyPair;
  private KademliaId kademliaId;

  public LegacyNodeId(KeyPair keyPair) {
    this.keyPair = keyPair;
  }

  /** Generates a new legacy identity (no HashCash proof of work — transition path only). */
  public static LegacyNodeId generate() {
    return new LegacyNodeId(generateECKeys());
  }

  public static KeyPair generateECKeys() {
    try {
      ECNamedCurveParameterSpec parameterSpec =
          ECNamedCurveTable.getParameterSpec("brainpoolp256r1");
      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("ECDH", "BC");
      keyPairGenerator.initialize(parameterSpec);
      return keyPairGenerator.generateKeyPair();
    } catch (NoSuchAlgorithmException
        | InvalidAlgorithmParameterException
        | NoSuchProviderException e) {
      throw new IllegalStateException("could not generate legacy brainpool keypair", e);
    }
  }

  public KeyPair getKeyPair() {
    return keyPair;
  }

  public boolean hasPrivate() {
    return keyPair != null && keyPair.getPrivate() != null;
  }

  public KademliaId getKademliaId() {
    if (kademliaId == null) {
      kademliaId = KademliaId.fromFirstBytes(Sha256Hash.create(exportPublic()).getBytes());
    }
    return kademliaId;
  }

  public byte[] exportPublic() {
    byte[] encoded = keyPair.getPublic().getEncoded();
    ByteBuffer buffer = ByteBuffer.wrap(encoded);
    byte[] bytes = new byte[PUBLIC_KEYLEN];
    buffer.position(PUBLIC_KEYLEN_LONG - PUBLIC_KEYLEN);
    buffer.get(bytes);
    return bytes;
  }

  public byte[] exportWithPrivate() {
    ByteBuffer buffer = ByteBuffer.allocate(PRIVATE_KEYLEN);
    byte[] encoded = keyPair.getPrivate().getEncoded();
    buffer.putInt(encoded.length);
    buffer.put(encoded);
    encoded = keyPair.getPublic().getEncoded();
    buffer.putInt(encoded.length);
    buffer.put(encoded);
    return buffer.array();
  }

  public static LegacyNodeId importPublic(byte[] bytes) {
    byte[] bytesFull = new byte[PUBLIC_KEYLEN_LONG];
    ByteBuffer.wrap(bytesFull).put(getCurveParametersForASN1Format()).put(bytes);

    EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(bytesFull);
    try {
      KeyFactory keyFactory = KeyFactory.getInstance("ECDH", "BC");
      PublicKey newPublicKey = keyFactory.generatePublic(publicKeySpec);
      return new LegacyNodeId(new KeyPair(newPublicKey, null));
    } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeySpecException e) {
      throw new IllegalArgumentException("could not import legacy public key", e);
    }
  }

  public static LegacyNodeId importWithPrivate(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    int len = buffer.getInt();
    byte[] privateKeyBytes = new byte[len];
    buffer.get(privateKeyBytes);
    EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(privateKeyBytes);

    len = buffer.getInt();
    byte[] publicKeyBytes = new byte[len];
    buffer.get(publicKeyBytes);
    EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyBytes);

    try {
      KeyFactory keyFactory = KeyFactory.getInstance("ECDH", "BC");
      PrivateKey newPrivateKey = keyFactory.generatePrivate(privateKeySpec);
      PublicKey newPublicKey = keyFactory.generatePublic(publicKeySpec);
      return new LegacyNodeId(new KeyPair(newPublicKey, newPrivateKey));
    } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeySpecException e) {
      throw new IllegalArgumentException("could not import legacy private key", e);
    }
  }

  private static synchronized byte[] getCurveParametersForASN1Format() {
    if (curveParametersASN1 == null) {
      PublicKey aPublic = generateECKeys().getPublic();
      ByteBuffer wrap = ByteBuffer.wrap(aPublic.getEncoded());
      byte[] bytes = new byte[PUBLIC_KEYLEN_LONG - PUBLIC_KEYLEN];
      wrap.get(bytes);
      curveParametersASN1 = bytes;
    }
    return curveParametersASN1;
  }

  /** Signs with SHA256withECDSA (variable-length DER signature) — legacy v1 format. */
  public byte[] sign(byte[] bytesToSign) {
    if (!hasPrivate()) {
      throw new IllegalStateException("this LegacyNodeId has no private key for signing!");
    }
    try {
      Signature ecdsa = Signature.getInstance("SHA256withECDSA");
      ecdsa.initSign(keyPair.getPrivate());
      ecdsa.update(bytesToSign);
      return ecdsa.sign();
    } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
      throw new IllegalStateException("legacy signing failed", e);
    }
  }

  /** Verifies a SHA256withECDSA signature — legacy v1 format. */
  public boolean verify(byte[] bytesToVerify, byte[] signature) {
    try {
      Signature ecdsa = Signature.getInstance("SHA256withECDSA");
      ecdsa.initVerify(keyPair.getPublic());
      ecdsa.update(bytesToVerify);
      return ecdsa.verify(signature);
    } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
      return false;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof LegacyNodeId id) {
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
    LegacyNodeId nodeId = hasPrivate ? importWithPrivate(bytes) : importPublic(bytes);
    keyPair = nodeId.getKeyPair();
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
