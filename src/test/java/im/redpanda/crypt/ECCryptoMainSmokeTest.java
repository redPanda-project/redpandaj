package im.redpanda.crypt;

import org.junit.Test;

import java.security.Security;

public class ECCryptoMainSmokeTest {
    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void main_executesWithoutException() throws Exception {
        // Exercise the demo main method to improve coverage of example code paths.
        ECCrypto.main(new String[]{});
    }
}

