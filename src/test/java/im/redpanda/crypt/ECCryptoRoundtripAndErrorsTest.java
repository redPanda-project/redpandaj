package im.redpanda.crypt;

import org.junit.Test;

import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.security.Security;

import static org.junit.Assert.*;

public class ECCryptoRoundtripAndErrorsTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void encryptDecrypt_roundtrip_withSharedSecret() {
        KeyPair a = ECCrypto.generateECKeys();
        KeyPair b = ECCrypto.generateECKeys();
        assertNotNull(a);
        assertNotNull(b);

        SecretKey keyA = ECCrypto.generateSharedSecret(a.getPrivate(), b.getPublic());
        SecretKey keyB = ECCrypto.generateSharedSecret(b.getPrivate(), a.getPublic());
        assertNotNull(keyA);
        assertNotNull(keyB);

        String msg = "hello crypto";
        String ct = ECCrypto.encryptString(keyA, msg);
        assertNotNull(ct);

        String pt = ECCrypto.decryptString(keyB, ct);
        assertEquals(msg, pt);
    }

    @Test
    public void decrypt_returnsNull_withWrongKey() {
        KeyPair a = ECCrypto.generateECKeys();
        KeyPair b = ECCrypto.generateECKeys();
        KeyPair c = ECCrypto.generateECKeys();

        SecretKey keyAB = ECCrypto.generateSharedSecret(a.getPrivate(), b.getPublic());
        SecretKey wrong = ECCrypto.generateSharedSecret(a.getPrivate(), c.getPublic());

        String ct = ECCrypto.encryptString(keyAB, "test");
        assertNotNull(ct);

        String ptWrong = ECCrypto.decryptString(wrong, ct);
        assertNull(ptWrong);
    }

    @Test
    public void decrypt_returnsNull_onTooShortCiphertext() {
        KeyPair a = ECCrypto.generateECKeys();
        KeyPair b = ECCrypto.generateECKeys();
        SecretKey key = ECCrypto.generateSharedSecret(a.getPrivate(), b.getPublic());

        // Less than 16-byte IV + 1 byte ciphertext (in hex => < 34 chars)
        String tooShort = "00"; // definitely too short
        String result = ECCrypto.decryptString(key, tooShort);
        assertNull(result);
    }

    @Test
    public void decrypt_returnsNull_onCorruptedCiphertext() {
        KeyPair a = ECCrypto.generateECKeys();
        KeyPair b = ECCrypto.generateECKeys();
        SecretKey key = ECCrypto.generateSharedSecret(a.getPrivate(), b.getPublic());

        // Construct 16-byte IV + 16-byte bogus ciphertext (all zeros) => 32 bytes => 64 hex chars
        byte[] zeros = new byte[32];
        String bogus = ECCrypto.bytesToHex(zeros);
        String result = ECCrypto.decryptString(key, bogus);
        assertNull(result);
    }

    @Test
    public void hexHelpers_roundtrip_andLengthVariant() {
        byte[] data = new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF};
        String hex = ECCrypto.bytesToHex(data);
        assertEquals("DEADBEEF", hex);

        byte[] back = ECCrypto.hexToBytes(hex);
        assertArrayEquals(data, back);

        // length-limited variant
        String firstTwoBytes = ECCrypto.bytesToHex(data, 2);
        assertEquals("DEAD", firstTwoBytes);
    }
}

