package im.redpanda.crypt;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.security.Security;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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

        assertThat(cipherOne, not(cipherTwo));
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
