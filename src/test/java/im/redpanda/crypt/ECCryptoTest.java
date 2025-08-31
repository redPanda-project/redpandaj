package im.redpanda.crypt;

import org.junit.Test;

import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.security.Security;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class ECCryptoTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void encrypt_samePlaintextShouldDiffer_withProperIVUse() {
        // This test expresses the security expectation that encrypting the same
        // plaintext twice with the same key should produce different ciphertexts
        // due to using unique IVs. Current implementation reuses a static IV.

        KeyPair a = ECCrypto.generateECKeys();
        KeyPair b = ECCrypto.generateECKeys();
        assertNotNull(a);
        assertNotNull(b);

        SecretKey keyA = ECCrypto.generateSharedSecret(a.getPrivate(), b.getPublic());
        SecretKey keyB = ECCrypto.generateSharedSecret(b.getPrivate(), a.getPublic());
        assertNotNull(keyA);
        assertNotNull(keyB);

        String plaintext = "Repeatable text";
        String c1 = ECCrypto.encryptString(keyA, plaintext);
        String c2 = ECCrypto.encryptString(keyA, plaintext);

        // With secure IV handling, ciphertexts should differ
        assertNotEquals(c1, c2);
    }
}

