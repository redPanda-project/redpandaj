package im.redpanda.crypt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.security.KeyPair;
import java.security.Security;
import javax.crypto.SecretKey;
import org.junit.Test;

public class ECCryptoMainSmokeTest {
  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void main_executesWithoutException() throws Exception {
    // Exercise the demo main method to improve coverage of example code paths.
    ECCrypto.main(new String[] {});

    KeyPair keyPairA = ECCrypto.generateECKeys();
    KeyPair keyPairB = ECCrypto.generateECKeys();
    assertNotNull(keyPairA);
    assertNotNull(keyPairB);

    SecretKey secretKeyA =
        ECCrypto.generateSharedSecret(keyPairA.getPrivate(), keyPairB.getPublic());
    SecretKey secretKeyB =
        ECCrypto.generateSharedSecret(keyPairB.getPrivate(), keyPairA.getPublic());
    assertNotNull(secretKeyA);
    assertNotNull(secretKeyB);

    String cipherText = ECCrypto.encryptString(secretKeyA, "hello");
    assertEquals("hello", ECCrypto.decryptString(secretKeyB, cipherText));
  }
}
