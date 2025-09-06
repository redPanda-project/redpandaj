package im.redpanda.docs;

import im.redpanda.crypt.ECCrypto;
import org.junit.Test;

import javax.crypto.SecretKey;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.Security;

import static org.junit.Assert.*;

public class ProtocolStep02ECCryptoDocsTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void documentationExistsAndStatesKeypoints() throws Exception {
        Path doc = Path.of("docs/protocol/02-eccrypto-iv-and-format.md");
        String text = Files.readString(doc);
        assertTrue(text.contains("Title: ECCrypto AES/GCM IV Handling and Ciphertext Format"));
        assertTrue(text.contains("Fresh IV"));
        assertTrue(text.contains("Encoding: encryptString returns hex(IV || ciphertext)"));
        assertTrue(text.contains("Decryption: decryptString expects hex(IV || ciphertext)"));
    }

    @Test
    public void decryptRoundTripAndRejectsMissingIvPrefix() {
        KeyPair a = ECCrypto.generateECKeys();
        KeyPair b = ECCrypto.generateECKeys();
        assertNotNull(a);
        assertNotNull(b);
        SecretKey key = ECCrypto.generateSharedSecret(a.getPrivate(), b.getPublic());
        assertNotNull(key);

        String plaintext = "hello";
        String ct = ECCrypto.encryptString(key, plaintext);
        assertNotNull(ct);
        String back = ECCrypto.decryptString(key, ct);
        assertEquals(plaintext, back);

        // Build a ciphertext without the IV prefix and ensure decryption fails gracefully
        byte[] all = ECCrypto.hexToBytes(ct);
        byte[] withoutIv = new byte[Math.max(0, all.length - 16)];
        System.arraycopy(all, 16, withoutIv, 0, withoutIv.length);
        String malformed = ECCrypto.bytesToHex(withoutIv);
        assertNull(ECCrypto.decryptString(key, malformed));
    }
}
