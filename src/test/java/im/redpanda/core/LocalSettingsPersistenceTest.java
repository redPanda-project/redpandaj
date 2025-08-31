package im.redpanda.core;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.security.Security;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class LocalSettingsPersistenceTest {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    private int port;

    @Before
    public void setUp() {
        // Use a unique, unlikely port to avoid clobbering other tests/files
        port = 49123;
        // Cleanup pre-existing file if any
        File f = new File(Settings.SAVE_DIR + "/localSettings" + port + ".dat");
        if (f.exists()) {
            // best effort
            f.delete();
        }
    }

    @After
    public void tearDown() {
        File f = new File(Settings.SAVE_DIR + "/localSettings" + port + ".dat");
        if (f.exists()) {
            f.delete();
        }
    }

    @Test
    public void saveAndLoadRoundtrip() {
        LocalSettings ls = new LocalSettings();
        ls.setUpdateTimestamp(123456789L);
        byte[] sig = new byte[]{1,2,3,4};
        ls.setUpdateSignature(sig);
        ls.setUpdateAndroidTimestamp(222L);
        byte[] asig = new byte[]{9,8,7};
        ls.setUpdateAndroidSignature(asig);

        ls.save(port);

        LocalSettings loaded = LocalSettings.load(port);
        assertNotNull(loaded);
        assertThat(loaded.getUpdateTimestamp(), is(123456789L));
        assertArrayEquals(sig, loaded.getUpdateSignature());
        assertThat(loaded.getUpdateAndroidTimestamp(), is(222L));
        assertArrayEquals(asig, loaded.getUpdateAndroidSignature());
        assertNotNull(loaded.getMyIdentity());
        assertNotNull(loaded.getNodeGraph());
        assertNotNull(loaded.getSystemUpTimeData());
    }
}

