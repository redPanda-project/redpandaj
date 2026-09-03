package im.redpanda.ops;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.core.ServerContext;
import java.io.File;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SettingsInitTest {

  private File redpandaJar;

  @BeforeEach
  void setup() {
    redpandaJar = new File("redpanda.jar");
    // Reset static flags to a known state
    Settings.seedNode = false;
    Settings.loadUpdates = false;
  }

  @AfterEach
  void cleanup() {
    if (redpandaJar.exists()) {
      // best effort cleanup
      redpandaJar.delete();
    }
  }

  @Test
  void whenRedpandaJarPresent_loadUpdatesTrue() throws Exception {
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
  void whenOnlyTargetJarAndDefaultPort_seedNodeTrue() throws Exception {
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
