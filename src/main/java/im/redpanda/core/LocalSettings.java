package im.redpanda.core;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import im.redpanda.identity.NodeId;
import im.redpanda.ops.Settings;
import im.redpanda.ops.SystemUpTimeData;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;

/**
 * The persisted state of this node: its identity keypair, the updater timestamps/signatures it
 * serves and the uptime window.
 *
 * <p>T136/TD174: the routing node graph is no longer part of this file but of {@code
 * data/nodeGraph<port>.json}, owned by {@code routing.graph}, so that the composition-root state
 * does not depend on the routing context. A file written before T136 still carries the graph in
 * {@code nodeGraph}; it is kept here as raw JSON ({@link #pendingLegacyNodeGraph()}) until the
 * routing side has written it to its own file, and only then dropped.
 *
 * <p>T117: written as explicit JSON ({@code data/localSettings<port>.json}), not as a Java object
 * stream any more. Java serialization pinned the fully qualified class names of {@code NodeId},
 * {@code Node}, {@code KademliaId} and this class into the file, so the package moves of T118 would
 * have made every deployed node fail to read its own identity (DDD review §5).
 *
 * <p><b>No migration path</b> (user decision 2026-09-01: there are no users yet). A node that finds
 * only the pre-T117 {@code localSettings<port>.dat} — or a settings file it cannot read — logs a
 * warning naming the file, generates a fresh identity and bootstraps via {@code
 * REDPANDA_KNOWN_NODES}. The old file is left on disk, exactly like the stale outbound stores of
 * T109.
 */
@Slf4j
public class LocalSettings {

  /** Header {@code format} of the settings file. */
  static final String FORMAT = "redpanda-local-settings";

  /** Header {@code version} of the settings file. */
  static final int VERSION = 1;

  private static final String FILE_PREFIX = "/localSettings";

  private NodeId myIdentity;

  private long updateTimestamp;
  private byte[] updateSignature;

  private long updateAndroidTimestamp;
  private byte[] updateAndroidSignature;

  /**
   * The {@code nodeGraph} member of a pre-T136 settings file, as raw JSON, until the routing side
   * has moved it into its own file and called {@link #dropLegacyNodeGraph()}. {@code null} when the
   * file held no graph (the empty placeholder written since T136) or once the migration is done.
   */
  private volatile JsonObject legacyNodeGraph;

  private SystemUpTimeData systemUpTimeData;

  public LocalSettings() {
    myIdentity = new NodeId();
    updateTimestamp = -1;
    systemUpTimeData = new SystemUpTimeData();
  }

  public void setUpdateSignature(byte[] updateSignature) {
    this.updateSignature = updateSignature;
  }

  public byte[] getUpdateSignature() {
    return updateSignature;
  }

  public byte[] getUpdateAndroidSignature() {
    return updateAndroidSignature;
  }

  public void setUpdateAndroidSignature(byte[] updateAndroidSignature) {
    this.updateAndroidSignature = updateAndroidSignature;
  }

  /**
   * The node graph embedded in a pre-T136 settings file, still waiting to be moved into its own
   * file, or {@code null}.
   */
  public JsonObject pendingLegacyNodeGraph() {
    JsonObject pending = legacyNodeGraph;
    return pending == null ? null : pending.deepCopy();
  }

  /**
   * Called once the embedded graph is safely in its own file (or found unreadable): from the next
   * {@link #save(int)} on, the settings file carries only an empty placeholder graph.
   */
  public void dropLegacyNodeGraph() {
    legacyNodeGraph = null;
  }

  /**
   * Writes the settings to disk atomically: the JSON document is built in memory first and the
   * resulting bytes go through a temporary file which only replaces the live file once it is
   * complete. Writing straight into the live file truncates it first, so any failure half way
   * through (a {@link java.util.ConcurrentModificationException} from a collection mutated by
   * another thread — REDPANDAJ-2E6 —, a full disk, ...) left behind a truncated file that {@link
   * #load(int)} cannot read, and the node silently generated a new identity on the next start.
   *
   * <p>Synchronized because both the {@code SaveJobs} job and the update handling call this, and
   * two saves running at once would write the same file.
   */
  public synchronized void save(int port) {
    File mkdirs = new File(Settings.SAVE_DIR);
    mkdirs.mkdir();

    try {
      byte[] encoded = new Gson().toJson(toJson()).getBytes(StandardCharsets.UTF_8);
      StateFormat.writeAtomically(settingsFile(port), tmpSettingsFile(port), encoded);
    } catch (IOException | RuntimeException ex) {
      // RuntimeException as well: unlike the removed object stream, which reported a broken object
      // graph as a NotSerializableException, the encoder throws unchecked (a vertex that is not a
      // Node, a ConcurrentModificationException, ...). Losing one save must never take the file
      // that holds the identity with it.
      log.info("error saving local settings", ex);
    }
  }

  private JsonObject toJson() {
    JsonObject json = StateFormat.document(FORMAT, VERSION);
    json.add("identity", NodeIdCodec.nodeIdToJson(myIdentity));
    json.addProperty("updateTimestamp", updateTimestamp);
    json.addProperty("updateSignature", StateFormat.base64(updateSignature));
    json.addProperty("updateAndroidTimestamp", updateAndroidTimestamp);
    json.addProperty("updateAndroidSignature", StateFormat.base64(updateAndroidSignature));

    JsonArray upHits = new JsonArray();
    for (Long hit : getSystemUpTimeData().snapshotUpHits()) {
      upHits.add(hit);
    }
    json.add("upHits", upHits);

    // Rollback safety: a pre-T136 build requires this member and generates a NEW identity if it
    // is missing. So the graph that has not been migrated yet is written back as it was read, and
    // afterwards an empty graph in the shape NodeGraphCodec reads takes its place.
    JsonObject pending = legacyNodeGraph;
    json.add("nodeGraph", pending != null ? pending : emptyNodeGraph());
    return json;
  }

  private static JsonObject emptyNodeGraph() {
    JsonObject graph = new JsonObject();
    graph.add("vertices", new JsonArray());
    graph.add("edges", new JsonArray());
    return graph;
  }

  /**
   * Loads the settings of {@code port}, or generates fresh ones.
   *
   * <p>Fresh settings mean a new node identity and no update signatures — the node re-bootstraps
   * from {@code REDPANDA_KNOWN_NODES} and gets a new KademliaId. That is the deliberate behaviour
   * for an unreadable, missing or pre-T117 file (user decision 2026-09-01: no users yet, so no
   * migration path is built). Nothing on disk is deleted.
   */
  public static LocalSettings load(int port) {
    File file = settingsFile(port);

    if (file.exists()) {
      try {
        return fromJson(StateFormat.parse(Files.readAllBytes(file.toPath()), FORMAT, VERSION));
      } catch (IOException | RuntimeException ex) {
        log.warn(
            "could not read {} ({}) - generating a NEW node identity and re-bootstrapping;"
                + " the unreadable file is kept",
            file,
            ex.toString());
      }
    } else {
      File legacy = legacySettingsFile(port);
      if (legacy.exists()) {
        log.warn(
            "found only the pre-T117 Java-serialized settings file {}; it is not read and not"
                + " migrated - generating a NEW node identity and re-bootstrapping. The file is"
                + " kept and can be deleted",
            legacy);
      } else {
        log.info("no settings file at {}, generating new LocalSettings", file);
      }
    }

    LocalSettings localSettings = new LocalSettings();
    localSettings.save(port);
    return localSettings;
  }

  private static LocalSettings fromJson(JsonObject json) throws IOException {
    LocalSettings settings = new LocalSettings();
    settings.myIdentity = NodeIdCodec.nodeIdFromJson(StateFormat.requireObject(json, "identity"));
    if (!settings.myIdentity.hasPrivate()) {
      throw new IOException("settings file holds no private identity key");
    }
    settings.updateTimestamp = StateFormat.optLong(json, "updateTimestamp", -1L);
    settings.updateSignature = StateFormat.optBase64(json, "updateSignature");
    settings.updateAndroidTimestamp = StateFormat.optLong(json, "updateAndroidTimestamp", 0L);
    settings.updateAndroidSignature = StateFormat.optBase64(json, "updateAndroidSignature");

    SortedSet<Long> upHits = new TreeSet<>();
    JsonElement upHitsJson = json.get("upHits");
    if (upHitsJson != null && upHitsJson.isJsonArray()) {
      for (JsonElement hit : upHitsJson.getAsJsonArray()) {
        upHits.add(hit.getAsLong());
      }
    }
    settings.systemUpTimeData = new SystemUpTimeData(upHits);

    // Kept as raw JSON only; decoding it is the routing side's job (TD174). A file written since
    // T136 carries an empty placeholder here, which is nothing to migrate.
    JsonElement nodeGraph = json.get("nodeGraph");
    if (nodeGraph != null && nodeGraph.isJsonObject()) {
      JsonElement vertices = nodeGraph.getAsJsonObject().get("vertices");
      if (vertices != null && vertices.isJsonArray() && !vertices.getAsJsonArray().isEmpty()) {
        settings.legacyNodeGraph = nodeGraph.getAsJsonObject();
      }
    }
    return settings;
  }

  /** The settings file of {@code port} in the explicit JSON format (T117). */
  public static File settingsFile(int port) {
    return new File(Settings.SAVE_DIR + FILE_PREFIX + port + ".json");
  }

  public static File tmpSettingsFile(int port) {
    return new File(Settings.SAVE_DIR + FILE_PREFIX + port + ".json.tmp");
  }

  /** The pre-T117 Java-serialized settings file. Never read, never deleted — only reported. */
  public static File legacySettingsFile(int port) {
    return new File(Settings.SAVE_DIR + FILE_PREFIX + port + ".dat");
  }

  public long getUpdateTimestamp() {
    return updateTimestamp;
  }

  public void setUpdateTimestamp(long updateTimestamp) {
    this.updateTimestamp = updateTimestamp;
  }

  public long getUpdateAndroidTimestamp() {
    return updateAndroidTimestamp;
  }

  public void setUpdateAndroidTimestamp(long updateAndroidTimestamp) {
    this.updateAndroidTimestamp = updateAndroidTimestamp;
  }

  public NodeId getMyIdentity() {
    return myIdentity;
  }

  public SystemUpTimeData getSystemUpTimeData() {
    if (systemUpTimeData == null) {
      systemUpTimeData = new SystemUpTimeData();
    }
    return systemUpTimeData;
  }
}
