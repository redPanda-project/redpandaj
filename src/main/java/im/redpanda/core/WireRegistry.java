package im.redpanda.core;

import im.redpanda.flaschenpost.FlaschenpostV2;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Renders the machine-generated part of the wire registry (see {@code wire-registry.md}).
 *
 * <p>The protocol contract used to live only in the constants themselves: top-level command bytes
 * in {@link Command}, garlic layer commands in {@link FlaschenpostV2}, and the protobuf definitions
 * in {@code src/main/proto}. This class derives a deterministic Markdown table from exactly those
 * three sources so that a human-readable registry can be checked against the code instead of being
 * maintained by hand (DDD review 2026-08-31, §6 P0).
 *
 * <p>The rendered text is stored as {@code src/main/resources/wire-registry.md} and verified by
 * {@code WireRegistryTest}; the same block is mirrored into the {@code docs} repository ({@code
 * docs/wire_registry.md}), where redpandaj is documented as the source.
 *
 * <p>Regenerate after changing a command byte or a {@code .proto} file:
 *
 * <pre>{@code
 * mvn -q compile
 * java -cp target/classes im.redpanda.core.WireRegistry
 * }</pre>
 */
public final class WireRegistry {

  /** Path of the generated registry inside the repository, relative to the project directory. */
  public static final String REGISTRY_PATH = "src/main/resources/wire-registry.md";

  /** Classpath location of the same file, for tests and runtime lookups. */
  public static final String REGISTRY_RESOURCE = "/wire-registry.md";

  /** Directory holding the protobuf definitions, relative to the project directory. */
  public static final String PROTO_DIR = "src/main/proto";

  /**
   * Matches a top-level {@code message}/{@code enum}/{@code service} declaration in a .proto file.
   */
  private static final Pattern PROTO_DEFINITION =
      Pattern.compile("^(message|enum|service)[ \\t]+([A-Za-z_][A-Za-z0-9_]*)");

  private WireRegistry() {}

  /**
   * Regenerates {@link #REGISTRY_PATH}.
   *
   * @param args optional: {@code [0]} project directory (default: the working directory)
   */
  public static void main(String[] args) throws IOException {
    Path projectDir = Path.of(args.length > 0 ? args[0] : ".");
    Path target = projectDir.resolve(REGISTRY_PATH);
    Files.writeString(target, render(projectDir.resolve(PROTO_DIR)), StandardCharsets.UTF_8);
    System.out.println("wrote " + target.toAbsolutePath().normalize());
  }

  /**
   * Renders the whole registry document.
   *
   * @param protoDir directory containing the {@code .proto} files
   */
  public static String render(Path protoDir) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("<!-- Redpanda wire registry - GENERATED FILE, do not edit by hand.\n");
    sb.append("     Sources: im.redpanda.core.Command, im.redpanda.flaschenpost.FlaschenpostV2,\n");
    sb.append("     ").append(PROTO_DIR).append("/*.proto\n");
    sb.append("     Regenerate: mvn -q compile && java -cp target/classes ")
        .append(WireRegistry.class.getName())
        .append("\n");
    sb.append("     Verified by: im.redpanda.core.WireRegistryTest -->\n\n");

    sb.append("## Top-level commands (`im.redpanda.core.Command`)\n\n");
    sb.append("First byte of every frame on a peer connection.\n\n");
    appendCommandTable(sb, byteConstants(Command.class, ""));

    sb.append("\n## Garlic layer commands (`im.redpanda.flaschenpost.FlaschenpostV2`)\n\n");
    sb.append(
        "First byte of a decrypted garlic layer, inside a `FLASCHENPOST_V2` (142) packet.\n\n");
    appendCommandTable(sb, byteConstants(FlaschenpostV2.class, "CMD_"));

    sb.append("\n## Protobuf definitions (`").append(PROTO_DIR).append("`)\n\n");
    appendProtoTable(sb, protoDir);
    return sb.toString();
  }

  private static void appendCommandTable(StringBuilder sb, List<ByteConstant> constants) {
    sb.append("| Constant | Dec | Hex |\n");
    sb.append("| --- | ---: | --- |\n");
    for (ByteConstant c : constants) {
      sb.append("| `")
          .append(c.name())
          .append("` | ")
          .append(c.value())
          .append(" | `")
          .append(String.format(Locale.ROOT, "0x%02X", c.value()))
          .append("` |\n");
    }
  }

  private static void appendProtoTable(StringBuilder sb, Path protoDir) throws IOException {
    sb.append("| File | Kind | Name |\n");
    sb.append("| --- | --- | --- |\n");
    for (Path file : protoFiles(protoDir)) {
      for (String[] definition : protoDefinitions(file)) {
        sb.append("| `")
            .append(file.getFileName())
            .append("` | ")
            .append(definition[0])
            .append(" | `")
            .append(definition[1])
            .append("` |\n");
      }
    }
  }

  private static List<Path> protoFiles(Path protoDir) throws IOException {
    try (Stream<Path> files = Files.list(protoDir)) {
      return files
          .filter(p -> p.getFileName().toString().endsWith(".proto"))
          .sorted(Comparator.comparing(p -> p.getFileName().toString()))
          .toList();
    }
  }

  /**
   * Returns the top-level declarations of a .proto file as {@code {kind, name}} pairs, in file
   * order.
   */
  private static List<String[]> protoDefinitions(Path file) throws IOException {
    List<String[]> definitions = new ArrayList<>();
    for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
      Matcher m = PROTO_DEFINITION.matcher(line);
      if (m.find()) {
        definitions.add(new String[] {m.group(1), m.group(2)});
      }
    }
    return definitions;
  }

  /**
   * Reflects the {@code public static final byte} constants of a class, ordered by value (the field
   * order the JVM reports is not specified, the byte values are stable).
   *
   * @param namePrefix only fields whose name starts with this prefix are collected ("" = all)
   */
  private static List<ByteConstant> byteConstants(Class<?> owner, String namePrefix) {
    List<ByteConstant> constants = new ArrayList<>();
    for (Field field : owner.getDeclaredFields()) {
      int mods = field.getModifiers();
      if (field.getType() != byte.class
          || !Modifier.isPublic(mods)
          || !Modifier.isStatic(mods)
          || !Modifier.isFinal(mods)
          || !field.getName().startsWith(namePrefix)) {
        continue;
      }
      try {
        constants.add(new ByteConstant(field.getName(), Byte.toUnsignedInt(field.getByte(null))));
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(
            "cannot read " + owner.getName() + "." + field.getName(), e);
      }
    }
    constants.sort(Comparator.comparingInt(ByteConstant::value).thenComparing(ByteConstant::name));
    return constants;
  }

  /** A single command byte: its constant name and its unsigned value. */
  private record ByteConstant(String name, int value) {}
}
