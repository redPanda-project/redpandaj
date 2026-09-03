package im.redpanda.core;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import im.redpanda.identity.NodeId;
import im.redpanda.ops.Settings;
import im.redpanda.ops.SystemUpTimeData;
import im.redpanda.routing.graph.Node;
import im.redpanda.routing.graph.NodeEdge;
import im.redpanda.routing.graph.NodeGraphCodec;
import im.redpanda.routing.graph.NodeStore;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import lombok.extern.slf4j.Slf4j;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;

/**
 * The persisted state of this node: its identity keypair, the updater timestamps/signatures it
 * serves, the uptime window and the node graph.
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

  private DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph;

  private SystemUpTimeData systemUpTimeData;

  /**
   * Read lock of the {@link NodeStore} that owns {@link #nodeGraph}, or {@code null} while no
   * NodeStore has adopted the graph (standalone uses such as {@code Updater}, and the window
   * between {@link #load(int)} and {@code NodeStore.build...}).
   *
   * <p>The lock is a property of the running process, not of the persisted settings, so it is not
   * part of the file. Set by {@link NodeStore#buildWithDiskCache(ServerContext)} / {@link
   * NodeStore#buildWithMemoryCacheOnly(ServerContext)} rather than passed to {@link #save(int)}, so
   * that every caller of {@code save()} is protected without having to know about the NodeStore.
   */
  private Lock nodeGraphLock;

  public LocalSettings() {
    myIdentity = new NodeId();
    updateTimestamp = -1;
    nodeGraph = new DefaultDirectedWeightedGraph<>(NodeEdge.class);
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
   * Sets the lock that guards {@link #nodeGraph} against concurrent mutation. See {@link
   * #nodeGraphLock}.
   *
   * @param nodeGraphLock the owning NodeStore's read lock, or {@code null} to detach
   */
  public synchronized void setNodeGraphLock(Lock nodeGraphLock) {
    this.nodeGraphLock = nodeGraphLock;
  }

  /**
   * Writes the settings to disk atomically: the JSON document is built in memory first and the
   * resulting bytes go through a temporary file which only replaces the live file once it is
   * complete. Writing straight into the live file truncates it first, so any failure half way
   * through (a {@link java.util.ConcurrentModificationException} from a collection mutated by
   * another thread — REDPANDAJ-2E6 —, a full disk, ...) left behind a truncated file that {@link
   * #load(int)} cannot read, and the node silently generated a new identity on the next start.
   *
   * <p>Encoding runs under the NodeStore read lock ({@link #nodeGraphLock}) because {@link
   * #nodeGraph} is the very graph {@code NodeStore#maintainNodes} mutates under the matching write
   * lock (REDPANDAJ-2DW). Without it the save itself still fails with a {@code
   * ConcurrentModificationException} — harmlessly since #282, but the graph never reaches the disk.
   * Only the in-memory encoding is covered; the file I/O and the fsync run outside the lock so that
   * a slow disk cannot stall the node's graph maintenance.
   *
   * <p>Lock order is {@code LocalSettings monitor -> NodeStore read lock}. Nothing may therefore
   * call {@code save()} while holding the NodeStore write lock; today no caller does.
   *
   * <p>Synchronized because both the {@code SaveJobs} job and the update handling call this, and
   * two saves running at once would write the same file.
   */
  public synchronized void save(int port) {
    File mkdirs = new File(Settings.SAVE_DIR);
    mkdirs.mkdir();

    try {
      byte[] encoded = encodeUnderGraphLock();
      StateFormat.writeAtomically(settingsFile(port), tmpSettingsFile(port), encoded);
    } catch (IOException | RuntimeException ex) {
      // RuntimeException as well: unlike the removed object stream, which reported a broken object
      // graph as a NotSerializableException, the encoder throws unchecked (a vertex that is not a
      // Node, a ConcurrentModificationException, ...). Losing one save must never take the file
      // that holds the identity with it.
      log.info("error saving local settings", ex);
    }
  }

  private byte[] encodeUnderGraphLock() {
    Lock lock = nodeGraphLock;
    if (lock != null) {
      lock.lock();
    }
    try {
      return new Gson().toJson(toJson()).getBytes(StandardCharsets.UTF_8);
    } finally {
      if (lock != null) {
        lock.unlock();
      }
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

    json.add("nodeGraph", NodeGraphCodec.toJson(nodeGraph));
    return json;
  }

  /**
   * Loads the settings of {@code port}, or generates fresh ones.
   *
   * <p>Fresh settings mean a new node identity, an empty node graph and no update signatures — the
   * node re-bootstraps from {@code REDPANDA_KNOWN_NODES} and gets a new KademliaId. That is the
   * deliberate behaviour for an unreadable, missing or pre-T117 file (user decision 2026-09-01: no
   * users yet, so no migration path is built). Nothing on disk is deleted.
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

    settings.nodeGraph = NodeGraphCodec.fromJson(StateFormat.requireObject(json, "nodeGraph"));
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

  public DefaultDirectedWeightedGraph<Node, NodeEdge> getNodeGraph() {
    return nodeGraph;
  }

  public SystemUpTimeData getSystemUpTimeData() {
    if (systemUpTimeData == null) {
      systemUpTimeData = new SystemUpTimeData();
    }
    return systemUpTimeData;
  }
}
