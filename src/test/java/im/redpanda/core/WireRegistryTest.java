package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * CI guard for the wire registry (DDD review 2026-08-31, §6 P0): the checked-in {@code
 * src/main/resources/wire-registry.md} must match what {@link WireRegistry} derives from {@link
 * Command}, {@link im.redpanda.routing.FlaschenpostV2} and {@code src/main/proto}.
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
   * Pins what the generator itself must emit for the garlic layer, independently of the checked-in
   * file: a dropped or renamed section would otherwise only be noticed once someone regenerates.
   * Matching happens inside the respective section, because the hex values 0x01-0x06 also occur in
   * the top-level command table.
   */
  @Test
  void renderPinsTheGarlicLayerCommandRows() throws IOException {
    String registry = WireRegistry.render(projectDir().resolve(WireRegistry.PROTO_DIR));
    String garlic = section(registry, "## Garlic layer commands");
    for (String row :
        List.of(
            "| `CMD_FORWARD` | 1 | `0x01` |",
            "| `CMD_DELIVER` | 2 | `0x02` |",
            "| `CMD_DELIVER_TAGGED` | 3 | `0x03` |",
            "| `CMD_DELIVER_ACKED` | 4 | `0x04` |",
            "| `CMD_RECORD_STORE` | 5 | `0x05` |",
            "| `CMD_RECORD_LOOKUP` | 6 | `0x06` |")) {
      assertTrue(garlic.contains(row), "garlic layer row missing from rendered registry: " + row);
    }
    assertTrue(
        section(registry, "## Top-level commands").contains("| `FLASCHENPOST_V2` | 142 | `0x8E` |"),
        "top-level command FLASCHENPOST_V2 = 142 missing from rendered registry");
  }

  /**
   * The proto table is produced by a line-based parser; this pins its contract on synthetic input
   * instead of relying on whatever the two real .proto files happen to contain today.
   */
  @Test
  void protoTableListsOnlyTopLevelDeclarations(@TempDir Path protoDir) throws IOException {
    Files.writeString(
        protoDir.resolve("fixture.proto"),
        """
        syntax = "proto3";
        // message CommentedOut
        message Outer {
          message Nested {
            int32 a = 1;
          }
          reserved 2, 15;
          oneof body {
            string text = 3;
          }
        }
        enum Kind {
          KIND_UNSPECIFIED = 0;
        }
        service Ignored {
        }
        """,
        StandardCharsets.UTF_8);

    String protoSection = section(WireRegistry.render(protoDir), "## Protobuf definitions");

    assertTrue(protoSection.contains("| `fixture.proto` | message | `Outer` |"), protoSection);
    assertTrue(protoSection.contains("| `fixture.proto` | enum | `Kind` |"), protoSection);
    assertTrue(protoSection.contains("| `fixture.proto` | service | `Ignored` |"), protoSection);
    assertFalse(protoSection.contains("Nested"), "indented nested types must not be listed");
    assertFalse(protoSection.contains("CommentedOut"), "commented-out types must not be listed");
    assertEquals(
        3,
        protoSection.lines().filter(l -> l.startsWith("| `fixture.proto`")).count(),
        protoSection);
  }

  private static String readCheckedInRegistry() throws IOException {
    try (InputStream in =
        WireRegistryTest.class.getResourceAsStream(WireRegistry.REGISTRY_RESOURCE)) {
      assertNotNull(in, "missing classpath resource " + WireRegistry.REGISTRY_RESOURCE);
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /** Returns the text of one {@code ## } section, up to the next one. */
  private static String section(String registry, String heading) {
    int start = registry.indexOf(heading);
    assertTrue(start >= 0, "missing section heading: " + heading);
    int end = registry.indexOf("\n## ", start + heading.length());
    return end < 0 ? registry.substring(start) : registry.substring(start, end);
  }

  /**
   * Surefire defaults each fork's working directory to the module directory, which is what makes
   * the {@code user.dir} fallback work; {@code basedir} is honoured first so an IDE run
   * configuration can point the test at the project explicitly.
   */
  private static Path projectDir() {
    return Path.of(System.getProperty("basedir", System.getProperty("user.dir", ".")));
  }
}
