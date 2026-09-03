package im.redpanda.core;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Base64;

/**
 * Shared plumbing of the explicit on-disk state format (T117, DDD review §5).
 *
 * <p>Node state used to be written with Java object serialization, which pins fully qualified class
 * names into every file: moving {@code im.redpanda.core.NodeId} to another package would have made
 * every deployed node fail to read its own identity. The replacement is plain JSON with an
 * explicit, hand-written mapping per file — no reflection over domain classes, so a class can be
 * renamed or moved without touching a byte on disk (this is what un-gates the repackaging in T118).
 *
 * <p>Every state file starts with the same two header fields:
 *
 * <pre>
 * {"format":"redpanda-local-settings","version":1, ...}
 * </pre>
 *
 * <p>{@code format} names the file, {@code version} its schema. A file whose header does not match
 * what the running code expects is treated exactly like a corrupt file: it is <b>not</b> migrated
 * and not deleted, the caller regenerates the state and logs the file name (user decision
 * 2026-09-01 — there are no users yet, so persisted node state may be dropped; the same rule T109
 * applied to the outbound stores).
 */
public final class StateFormat {

  private static final String FORMAT_KEY = "format";
  private static final String VERSION_KEY = "version";

  private StateFormat() {}

  /** Starts a state document with the {@code format}/{@code version} header. */
  public static JsonObject document(String format, int version) {
    JsonObject root = new JsonObject();
    root.addProperty(FORMAT_KEY, format);
    root.addProperty(VERSION_KEY, version);
    return root;
  }

  /**
   * Parses {@code json} and verifies the header.
   *
   * @throws IOException if the bytes are not JSON, are not an object, or carry a different
   *     format/version — the caller regenerates in all of these cases
   */
  public static JsonObject parse(byte[] json, String format, int version) throws IOException {
    final JsonElement parsed;
    try {
      parsed = JsonParser.parseString(new String(json, StandardCharsets.UTF_8));
    } catch (JsonSyntaxException e) {
      throw new IOException("not valid JSON", e);
    }
    if (parsed == null || !parsed.isJsonObject()) {
      throw new IOException("state file is not a JSON object");
    }
    JsonObject root = parsed.getAsJsonObject();
    String actualFormat = optString(root, FORMAT_KEY);
    if (!format.equals(actualFormat)) {
      throw new IOException("expected format '" + format + "' but found '" + actualFormat + "'");
    }
    // optInt rather than getAsInt: a version member that is JSON null, a string or an object must
    // read as "unreadable file", not as an unchecked exception escaping a method that declares
    // IOException.
    if (optInt(root, VERSION_KEY, -1) != version) {
      throw new IOException(
          "expected " + format + " version " + version + " but found " + root.get(VERSION_KEY));
    }
    return root;
  }

  /** Base64 of {@code bytes}, or {@code null} for a {@code null} array (kept as a JSON null). */
  public static String base64(byte[] bytes) {
    return bytes == null ? null : Base64.getEncoder().encodeToString(bytes);
  }

  /** Reads a Base64 member, {@code null} if absent or JSON null. */
  public static byte[] optBase64(JsonObject object, String member) throws IOException {
    JsonElement element = object.get(member);
    if (element == null || element.isJsonNull()) {
      return null;
    }
    try {
      return Base64.getDecoder().decode(element.getAsString());
    } catch (IllegalArgumentException | UnsupportedOperationException | IllegalStateException e) {
      throw new IOException("member '" + member + "' is not valid Base64", e);
    }
  }

  /** Reads a required Base64 member of exactly {@code expectedLength} bytes. */
  public static byte[] requireBase64(JsonObject object, String member, int expectedLength)
      throws IOException {
    byte[] bytes = optBase64(object, member);
    if (bytes == null) {
      throw new IOException("missing member '" + member + "'");
    }
    if (bytes.length != expectedLength) {
      throw new IOException(
          "member '" + member + "' must be " + expectedLength + " bytes but was " + bytes.length);
    }
    return bytes;
  }

  /** Reads a required object member. */
  public static JsonObject requireObject(JsonObject object, String member) throws IOException {
    JsonElement element = object.get(member);
    if (element == null || !element.isJsonObject()) {
      throw new IOException("missing object member '" + member + "'");
    }
    return element.getAsJsonObject();
  }

  /** Reads a long member, {@code fallback} if absent or JSON null. */
  public static long optLong(JsonObject object, String member, long fallback) throws IOException {
    JsonElement element = object.get(member);
    if (element == null || element.isJsonNull()) {
      return fallback;
    }
    try {
      return element.getAsLong();
    } catch (NumberFormatException | UnsupportedOperationException | IllegalStateException e) {
      throw new IOException("member '" + member + "' is not a number", e);
    }
  }

  /**
   * Reads an int member, {@code fallback} if absent or JSON null. A value that does not fit into an
   * int is an unreadable file, not an {@code ArithmeticException}.
   */
  public static int optInt(JsonObject object, String member, int fallback) throws IOException {
    long value = optLong(object, member, fallback);
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
      throw new IOException("member '" + member + "' does not fit into an int: " + value);
    }
    return (int) value;
  }

  /** Reads a double member, {@code fallback} if absent or JSON null. */
  public static double optDouble(JsonObject object, String member, double fallback)
      throws IOException {
    JsonElement element = object.get(member);
    if (element == null || element.isJsonNull()) {
      return fallback;
    }
    try {
      return element.getAsDouble();
    } catch (NumberFormatException | UnsupportedOperationException | IllegalStateException e) {
      throw new IOException("member '" + member + "' is not a number", e);
    }
  }

  /** Reads a boolean member, {@code fallback} if absent or JSON null. */
  public static boolean optBoolean(JsonObject object, String member, boolean fallback)
      throws IOException {
    JsonElement element = object.get(member);
    if (element == null || element.isJsonNull()) {
      return fallback;
    }
    try {
      return element.getAsBoolean();
    } catch (UnsupportedOperationException | IllegalStateException e) {
      throw new IOException("member '" + member + "' is not a boolean", e);
    }
  }

  private static String optString(JsonObject object, String member) {
    JsonElement element = object.get(member);
    if (element == null || !element.isJsonPrimitive()) {
      return null;
    }
    return element.getAsString();
  }

  /**
   * Writes {@code bytes} to {@code file} through {@code file + ".tmp"}: the temporary file is
   * fsynced and only then atomically renamed over the live file. Writing in place truncates the
   * live file first, so any failure half way through used to leave a file that cannot be read — for
   * the settings that means a silently regenerated node identity on the next start.
   *
   * <p>The temporary file is removed in every case; on success the rename has already taken it
   * away.
   */
  public static void writeAtomically(File file, File tmpFile, byte[] bytes) throws IOException {
    try {
      try (FileOutputStream out = new FileOutputStream(tmpFile)) {
        out.write(bytes);
        out.flush();
        out.getFD().sync();
      }
      Files.move(
          tmpFile.toPath(),
          file.toPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);
    } finally {
      if (tmpFile.exists() && !tmpFile.delete()) {
        Log.put("could not delete temporary state file " + tmpFile, 20);
      }
    }
  }
}
