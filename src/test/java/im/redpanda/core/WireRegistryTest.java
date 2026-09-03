package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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

  /**
   * Pins the garlic layer rows and the garlic carrier command, so a rename or a dropped section
   * cannot leave the registry (and this test) vacuously satisfied. Matching happens inside the
   * respective section: the hex values 0x01-0x06 also occur in the top-level table.
   */
  @Test
  void registryPinsTheGarlicLayerCommandRows() throws IOException {
    String registry = readCheckedInRegistry();
    String garlic = section(registry, "## Garlic layer commands");
    for (String row :
        List.of(
            "| `CMD_FORWARD` | 1 | `0x01` |",
            "| `CMD_DELIVER` | 2 | `0x02` |",
            "| `CMD_DELIVER_TAGGED` | 3 | `0x03` |",
            "| `CMD_DELIVER_ACKED` | 4 | `0x04` |",
            "| `CMD_RECORD_STORE` | 5 | `0x05` |",
            "| `CMD_RECORD_LOOKUP` | 6 | `0x06` |")) {
      assertTrue(garlic.contains(row), "garlic layer row missing from registry: " + row);
    }
    assertTrue(
        section(registry, "## Top-level commands").contains("| `FLASCHENPOST_V2` | 142 | `0x8E` |"),
        "top-level command FLASCHENPOST_V2 = 142 missing from registry");
  }

  /** Returns the text of one {@code ## } section, up to the next one. */
  private static String section(String registry, String heading) {
    int start = registry.indexOf(heading);
    assertTrue(start >= 0, "missing section heading: " + heading);
    int end = registry.indexOf("\n## ", start + heading.length());
    return end < 0 ? registry.substring(start) : registry.substring(start, end);
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
