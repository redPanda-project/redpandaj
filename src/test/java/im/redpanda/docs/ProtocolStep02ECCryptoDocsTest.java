package im.redpanda.docs;

import static org.junit.Assert.*;

import im.redpanda.crypt.CryptoUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import javax.crypto.AEADBadTagException;
import org.junit.Test;

public class ProtocolStep02ECCryptoDocsTest {

  @Test
  public void documentationExistsAndStatesKeypoints() throws Exception {
    Path doc = Path.of("docs/protocol/02-eccrypto-iv-and-format.md");
    String text = Files.readString(doc);
    assertTrue(
        text.contains("Title: CryptoUtils AES-256-GCM Nonce Handling and Ciphertext Format"));
    assertTrue(text.contains("Fresh nonce"));
    assertTrue(text.contains("ciphertext || tag"));
    assertTrue(text.contains("AEADBadTagException"));
  }

  @Test
  public void gcmRoundTripAndTagFailure() throws Exception {
    SecureRandom random = new SecureRandom();
    byte[] key = new byte[CryptoUtils.AES_KEY_LEN];
    byte[] nonce = new byte[CryptoUtils.GCM_NONCE_LEN];
    random.nextBytes(key);
    random.nextBytes(nonce);
    byte[] plaintext = "hello".getBytes();
    byte[] aad = "aad".getBytes();

    byte[] ciphertext = CryptoUtils.encryptGcm(key, nonce, plaintext, aad);
    assertEquals(plaintext.length + CryptoUtils.GCM_TAG_LEN, ciphertext.length);
    assertArrayEquals(plaintext, CryptoUtils.decryptGcm(key, nonce, ciphertext, aad));

    // a flipped bit in the ciphertext must fail authentication
    ciphertext[0] ^= 0x01;
    assertThrows(
        AEADBadTagException.class, () -> CryptoUtils.decryptGcm(key, nonce, ciphertext, aad));
  }
}
