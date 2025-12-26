package im.redpanda.docs;

import static org.junit.Assert.*;

import im.redpanda.crypt.ECCrypto;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.Security;
import javax.crypto.SecretKey;
import org.junit.Test;

public class ProtocolStep03ExceptionHandlingDocsTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void documentationExistsAndStatesKeypoints() throws Exception {
    Path doc = Path.of("docs/protocol/03-exception-handling.md");
    String text = Files.readString(doc);
    assertTrue(text.contains("Title: Exception Handling Adjustments in Crypto"));
    assertTrue(text.contains("Removed ShortBufferException"));
    assertTrue(text.contains("encryptString and decryptString catch"));
  }

  @Test
  public void decryptReturnsNullForMalformedInput() {
    KeyPair a = ECCrypto.generateECKeys();
    KeyPair b = ECCrypto.generateECKeys();
    SecretKey key = ECCrypto.generateSharedSecret(a.getPrivate(), b.getPublic());
    String tooShort = "00"; // malformed hex with insufficient bytes for IV
    String res = ECCrypto.decryptString(key, tooShort);
    assertNull(res);
  }
}
