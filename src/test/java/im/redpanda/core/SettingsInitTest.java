package im.redpanda.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class SettingsInitTest {

    private File redpandaJar;

    @Before
    public void setup() {
        redpandaJar = new File("redpanda.jar");
        // Reset static flags to a known state
        Settings.seedNode = false;
        Settings.loadUpdates = false;
    }

    @After
    public void cleanup() {
        if (redpandaJar.exists()) {
            // best effort cleanup
            redpandaJar.delete();
        }
    }

    @Test
    public void whenRedpandaJarPresent_loadUpdatesTrue() throws IOException {
        // Ensure file exists
        if (!redpandaJar.exists()) {
            assertTrue(redpandaJar.createNewFile());
        }

        ServerContext ctx = new ServerContext();
        ctx.setPort(Settings.DEFAULT_PORT);

        Settings.init(ctx);

        assertTrue(Settings.isLoadUpdates());
        assertFalse(Settings.isSeedNode());
    }

    @Test
    public void whenOnlyTargetJarAndDefaultPort_seedNodeTrue() throws IOException {
        // Ensure no redpanda.jar in cwd
        if (redpandaJar.exists()) {
            assertTrue(redpandaJar.delete());
        }
        // Create a placeholder target/redpanda.jar so Settings.init sees a packaged jar
        File targetDir = new File("target");
        if (!targetDir.exists()) {
            assertTrue(targetDir.mkdirs());
        }
        File packaged = new File(targetDir, "redpanda.jar");
        if (!packaged.exists()) {
            assertTrue(packaged.createNewFile());
        }

        ServerContext ctx = new ServerContext();
        ctx.setPort(Settings.DEFAULT_PORT);

        Settings.init(ctx);

        assertFalse(Settings.isLoadUpdates());
        assertTrue(Settings.isSeedNode());

        // cleanup placeholder
        assertTrue(packaged.delete());
    }
}
