package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * CI guard for the wire registry (DDD review 2026-08-31, §6 P0): the checked-in {@code
 * src/main/resources/wire-registry.md} must match what {@link WireRegistry} derives from {@link
 * Command}, {@link im.redpanda.flaschenpost.FlaschenpostV2} and {@code src/main/proto}.
 *
 * <p>The same registry block is mirrored into the {@code docs} repository ({@code
 * docs/wire_registry.md}); this test is what makes redpandaj the source of truth for it.
 */
class WireRegistryTest {

  @Test
  void checkedInRegistryMatchesTheCode() throws IOException {
    Path protoDir = projectDir().resolve(WireRegistry.PROTO_DIR);
    assertTrue(
        Files.isDirectory(protoDir),
        "proto directory not found at " + protoDir.toAbsolutePath() + " (working dir issue?)");

    String expected = WireRegistry.render(protoDir);
    String actual = readCheckedInRegistry();

    assertEquals(
        expected,
        actual,
        "The checked-in wire registry is out of date. Regenerate it with:\n"
            + "  mvn -q compile && java -cp target/classes "
            + WireRegistry.class.getName()
            + "\n"
            + "and mirror the same content into docs/wire_registry.md (docs repo).\n");
  }

  /** Guards the two facts the registry hard-codes about itself, so a rename cannot go unnoticed. */
  @Test
  void registryCoversTheGarlicLayerCommandRange() throws IOException {
    String registry = readCheckedInRegistry();
    for (int cmd = 0x01; cmd <= 0x06; cmd++) {
      String hex = String.format("`0x%02X`", cmd);
      assertTrue(registry.contains(hex), "garlic layer command " + hex + " missing from registry");
    }
    assertTrue(
        registry.contains("`FLASCHENPOST_V2` | 142"),
        "top-level command FLASCHENPOST_V2 = 142 missing from registry");
  }

  private static String readCheckedInRegistry() throws IOException {
    try (InputStream in =
        WireRegistryTest.class.getResourceAsStream(WireRegistry.REGISTRY_RESOURCE)) {
      assertNotNull(in, "missing classpath resource " + WireRegistry.REGISTRY_RESOURCE);
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /**
   * Surefire runs the forks with the project directory as working directory and also exports it as
   * {@code basedir}; both are used so the test also works from an IDE run configuration.
   */
  private static Path projectDir() {
    return Path.of(System.getProperty("basedir", System.getProperty("user.dir", ".")));
  }
}
