package im.redpanda.crypt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.security.KeyPair;
import java.security.Security;
import java.util.Arrays;
import javax.crypto.SecretKey;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.BeforeClass;
import org.junit.Test;

public class ECCryptoTest {

  @BeforeClass
  public static void installProvider() {
    Security.addProvider(new BouncyCastleProvider());
  }

  @Test
  public void encryptsWithFreshIvEveryTime() {
    KeyPair a = ECCrypto.generateECKeys();
    KeyPair b = ECCrypto.generateECKeys();

    SecretKey shared = ECCrypto.generateSharedSecret(a.getPrivate(), b.getPublic());

    String cipherOne = ECCrypto.encryptString(shared, "hello");
    String cipherTwo = ECCrypto.encryptString(shared, "hello");

    assertThat(cipherOne).isNotEqualTo(cipherTwo);
    assertEquals("hello", ECCrypto.decryptString(shared, cipherOne));
    assertEquals("hello", ECCrypto.decryptString(shared, cipherTwo));
  }

  @Test
  public void decryptFailsWhenIvIsTampered() {
    KeyPair a = ECCrypto.generateECKeys();
    KeyPair b = ECCrypto.generateECKeys();

    SecretKey shared = ECCrypto.generateSharedSecret(a.getPrivate(), b.getPublic());

    String ciphertext = ECCrypto.encryptString(shared, "payload");
    byte[] bytes = ECCrypto.hexToBytes(ciphertext);
    Arrays.fill(bytes, 0, 16, (byte) 0xFF);

    assertNull(ECCrypto.decryptString(shared, ECCrypto.bytesToHex(bytes)));
  }
}
