package im.redpanda.crypt;

import java.security.GeneralSecurityException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.crypto.agreement.X25519Agreement;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.generators.HKDFBytesGenerator;
import org.bouncycastle.crypto.params.HKDFParameters;
import org.bouncycastle.crypto.params.X25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;

/**
 * MS03 crypto primitives: X25519 key agreement, HKDF-SHA256 key derivation and AES-256-GCM
 * authenticated encryption. All higher-level protocol code (NodeId, GarlicMessage v2, TCP v23)
 * builds on these helpers — no other crypto primitives may be used outside the deprecated v22
 * legacy path in {@link im.redpanda.crypt.legacy}.
 */
public final class CryptoUtils {

  /** AES-256 key length in bytes. */
  public static final int AES_KEY_LEN = 32;

  /** GCM standard nonce length in bytes. */
  public static final int GCM_NONCE_LEN = 12;

  /** GCM authentication tag length in bytes. */
  public static final int GCM_TAG_LEN = 16;

  /** X25519 shared secret / key length in bytes. */
  public static final int X25519_KEY_LEN = 32;

  private CryptoUtils() {}

  /** Computes the raw X25519 shared secret (32 bytes). */
  public static byte[] x25519(
      X25519PrivateKeyParameters privateKey, X25519PublicKeyParameters publicKey) {
    X25519Agreement agreement = new X25519Agreement();
    agreement.init(privateKey);
    byte[] shared = new byte[agreement.getAgreementSize()];
    agreement.calculateAgreement(publicKey, shared, 0);
    return shared;
  }

  /** HKDF-SHA256: derives {@code outLen} bytes from the input key material. */
  public static byte[] hkdfSha256(byte[] ikm, byte[] salt, byte[] info, int outLen) {
    HKDFBytesGenerator hkdf = new HKDFBytesGenerator(new SHA256Digest());
    hkdf.init(new HKDFParameters(ikm, salt, info));
    byte[] out = new byte[outLen];
    hkdf.generateBytes(out, 0, outLen);
    return out;
  }

  /**
   * AES-256-GCM encryption. Returns {@code ciphertext || tag(16)}.
   *
   * @param aad additional authenticated data, may be {@code null}
   */
  public static byte[] encryptGcm(byte[] key, byte[] nonce, byte[] plaintext, byte[] aad)
      throws GeneralSecurityException {
    Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(
        Cipher.ENCRYPT_MODE,
        new SecretKeySpec(key, "AES"),
        new GCMParameterSpec(GCM_TAG_LEN * 8, nonce));
    if (aad != null) {
      cipher.updateAAD(aad);
    }
    return cipher.doFinal(plaintext);
  }

  /**
   * AES-256-GCM decryption of {@code ciphertext || tag(16)}.
   *
   * @param aad additional authenticated data, may be {@code null}
   * @throws javax.crypto.AEADBadTagException if the authentication tag does not verify
   */
  public static byte[] decryptGcm(byte[] key, byte[] nonce, byte[] ciphertextAndTag, byte[] aad)
      throws GeneralSecurityException {
    Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(
        Cipher.DECRYPT_MODE,
        new SecretKeySpec(key, "AES"),
        new GCMParameterSpec(GCM_TAG_LEN * 8, nonce));
    if (aad != null) {
      cipher.updateAAD(aad);
    }
    return cipher.doFinal(ciphertextAndTag);
  }

  /** Counts the leading zero bits of the given bytes (HashCash proof-of-work metric). */
  public static int countLeadingZeroBits(byte[] bytes) {
    int bits = 0;
    for (byte b : bytes) {
      if (b == 0) {
        bits += 8;
        continue;
      }
      bits += Integer.numberOfLeadingZeros(b & 0xFF) - 24;
      break;
    }
    return bits;
  }
}
