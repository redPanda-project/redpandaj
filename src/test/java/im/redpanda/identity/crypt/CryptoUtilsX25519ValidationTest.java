package im.redpanda.identity.crypt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.InvalidKeyException;
import java.security.SecureRandom;
import org.bouncycastle.crypto.params.X25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for L2 (bug hunt 2026-07-26): X25519 outputs feed HKDF at every call site, so
 * degenerate peer public keys — the small-order points and the other inputs that make the agreement
 * all-zero regardless of our private key — must be rejected before the KDF ever sees them (RFC 7748
 * §6.1 contributory behaviour).
 */
class CryptoUtilsX25519ValidationTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  /** {@code u = 0} — the canonical degenerate input. */
  private static final byte[] ALL_ZERO = new byte[32];

  /** {@code u = 1}, order 4. */
  private static final byte[] U_ONE = withFirstByte(1);

  /** An order-8 point (RFC 7748 §6.1 / the classic small-order test vector). */
  private static final byte[] ORDER_EIGHT =
      new byte[] {
        (byte) 0xe0,
        (byte) 0xeb,
        0x7a,
        0x7c,
        0x3b,
        0x41,
        (byte) 0xb8,
        (byte) 0xae,
        0x16,
        0x56,
        (byte) 0xe3,
        (byte) 0xfa,
        (byte) 0xf1,
        (byte) 0x9f,
        (byte) 0xc4,
        0x6a,
        (byte) 0xda,
        0x09,
        (byte) 0x8d,
        (byte) 0xeb,
        (byte) 0x9c,
        0x32,
        (byte) 0xb1,
        (byte) 0xfd,
        (byte) 0x86,
        0x62,
        0x05,
        0x16,
        0x5f,
        0x49,
        (byte) 0xb8,
        0x00
      };

  /** {@code u = p = 2^255 - 19}, i.e. 0 in disguise. */
  private static final byte[] U_P = fieldPrime();

  @Test
  void x25519_rejectsDegeneratePeerKeys() {
    for (byte[] degenerate : new byte[][] {ALL_ZERO, U_ONE, ORDER_EIGHT, U_P}) {
      assertThatThrownBy(
              () ->
                  CryptoUtils.x25519(
                      new X25519PrivateKeyParameters(RANDOM),
                      new X25519PublicKeyParameters(degenerate, 0)))
          .as("degenerate peer key must never reach the KDF")
          .isInstanceOf(InvalidKeyException.class);
    }
  }

  /**
   * Interop guard: a conforming peer derives its public key from a random scalar, which is never a
   * degenerate point (probability ~2^-125). This is the evidence that the backend-side check cannot
   * reject a well-behaved current client — the mobile side derives its keys the same way.
   */
  @Test
  void x25519_acceptsEveryHonestlyGeneratedKeypair() throws Exception {
    for (int i = 0; i < 500; i++) {
      X25519PrivateKeyParameters ours = new X25519PrivateKeyParameters(RANDOM);
      X25519PrivateKeyParameters theirs = new X25519PrivateKeyParameters(RANDOM);

      byte[] a = CryptoUtils.x25519(ours, theirs.generatePublicKey());
      byte[] b = CryptoUtils.x25519(theirs, ours.generatePublicKey());

      assertThat(a).isEqualTo(b);
      assertThat(a).hasSize(CryptoUtils.X25519_KEY_LEN);
    }
  }

  @Test
  void x25519_isStillAPlainAgreementForNormalKeys() {
    X25519PrivateKeyParameters ours = new X25519PrivateKeyParameters(RANDOM);
    X25519PrivateKeyParameters theirs = new X25519PrivateKeyParameters(RANDOM);

    assertThatCode(() -> CryptoUtils.x25519(ours, theirs.generatePublicKey()))
        .doesNotThrowAnyException();
  }

  private static byte[] withFirstByte(int value) {
    byte[] bytes = new byte[32];
    bytes[0] = (byte) value;
    return bytes;
  }

  private static byte[] fieldPrime() {
    byte[] bytes = new byte[32];
    bytes[0] = (byte) 0xed;
    for (int i = 1; i < 31; i++) {
      bytes[i] = (byte) 0xff;
    }
    bytes[31] = (byte) 0x7f;
    return bytes;
  }
}
