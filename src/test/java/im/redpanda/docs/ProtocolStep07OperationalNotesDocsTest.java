package im.redpanda.docs;

import im.redpanda.crypt.ECCrypto;
import org.junit.Test;

import javax.crypto.SecretKey;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.Security;

import static org.junit.Assert.*;

public class ProtocolStep07OperationalNotesDocsTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void documentationExistsAndStatesKeypoints() throws Exception {
        Path doc = Path.of("docs/protocol/07-operational-notes.md");
        String text = Files.readString(doc);
        assertTrue(text.contains("Title: Operational Notes"));
        assertTrue(text.contains("hex(IV||ciphertext)"));
        assertTrue(text.contains("Log hygiene"));
    }

    @Test
    public void decryptFailsWithoutIvPrefix_succeedsWithProperFormat() {
        KeyPair a = ECCrypto.generateECKeys();
        KeyPair b = ECCrypto.generateECKeys();
        SecretKey key = ECCrypto.generateSharedSecret(a.getPrivate(), b.getPublic());
        String pt = "doc-check";
        String ct = ECCrypto.encryptString(key, pt);
        String dec = ECCrypto.decryptString(key, ct);
        assertEquals(pt, dec);

        byte[] all = ECCrypto.hexToBytes(ct);
        byte[] noIv = new byte[Math.max(0, all.length - 16)];
        System.arraycopy(all, 16, noIv, 0, noIv.length);
        assertNull(ECCrypto.decryptString(key, ECCrypto.bytesToHex(noIv)));
    }
}
