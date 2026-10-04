package im.redpanda.routing.graph;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import im.redpanda.core.LocalSettings;
import im.redpanda.core.StateFormat;
import im.redpanda.ops.Settings;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.Lock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;

/**
 * The persisted routing node graph, {@code data/nodeGraph<port>.json} (T136/TD174).
 *
 * <p>Until T136 the graph was a member of {@code LocalSettings}, which made the composition-root
 * state depend on this package. Same JSON mapping as before ({@link NodeGraphCodec}), now in its
 * own file under its own header.
 *
 * <p><b>Migration:</b> a settings file written before T136 still carries the graph. When there is
 * no graph file yet, {@link #load(int, LocalSettings)} takes that embedded graph, writes it into
 * this file right away, and only once that write succeeded tells the settings to drop it — so a
 * crash at any point leaves the graph in at least one of the two files. Once a graph file exists it
 * always wins and a still-embedded graph is dropped: the settings file can lag behind (it is only
 * rewritten at the next settings save, and the standalone {@code Updater} writes a pending embedded
 * graph back verbatim), so the embedded copy is never known to be the newer one. After a rollback
 * to a pre-T136 build and forward again, the node therefore resumes from the graph it had before
 * the rollback — soft state, rebuilt from the network either way.
 */
public final class NodeGraphFile {

  private static final Logger logger = LogManager.getLogger();

  /** Header {@code format} of the graph file. */
  static final String FORMAT = "redpanda-node-graph";

  /** Header {@code version} of the graph file. */
  static final int VERSION = 1;

  private static final String FILE_PREFIX = "/nodeGraph";

  /**
   * Serializes writes of the graph file. Static rather than per store: a store that replaced a
   * broken one shares its predecessor's graph, and either may be asked to save it.
   */
  private static final Object WRITE_LOCK = new Object();

  private NodeGraphFile() {}

  public static File file(int port) {
    return new File(Settings.SAVE_DIR + FILE_PREFIX + port + ".json");
  }

  public static File tmpFile(int port) {
    return new File(Settings.SAVE_DIR + FILE_PREFIX + port + ".json.tmp");
  }

  /**
   * Loads the node graph of {@code port}: the graph file if there is one, otherwise the graph still
   * embedded in a pre-T136 {@code settings} file (migrating it), otherwise an empty graph. Never
   * throws: the graph is soft state the node rebuilds from the network, so an unreadable file is
   * logged and replaced at the next save.
   *
   * @param settings the node's settings, or {@code null}
   */
  static DefaultDirectedWeightedGraph<Node, NodeEdge> load(int port, LocalSettings settings) {
    File file = file(port);
    JsonObject legacy = settings == null ? null : settings.pendingLegacyNodeGraph();
    if (legacy != null && file.exists()) {
      logger.info(
          "{} exists, ignoring the node graph still embedded in {}",
          file,
          LocalSettings.settingsFile(port));
      settings.dropLegacyNodeGraph();
    } else if (legacy != null) {
      try {
        DefaultDirectedWeightedGraph<Node, NodeEdge> graph = NodeGraphCodec.fromJson(legacy);
        if (save(port, graph, null, settings)) {
          logger.info(
              "migrated the node graph ({} nodes, {} edges) out of {} into {} (TD174)",
              graph.vertexSet().size(),
              graph.edgeSet().size(),
              LocalSettings.settingsFile(port),
              file(port));
        }
        return graph;
      } catch (IOException | RuntimeException e) {
        logger.warn(
            "could not read the node graph embedded in {}, dropping it: {}",
            LocalSettings.settingsFile(port),
            e.toString());
        settings.dropLegacyNodeGraph();
      }
    }

    if (!file.exists()) {
      logger.info("no node graph file at {}, starting with an empty graph", file);
      return new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    }
    try {
      JsonObject json = StateFormat.parse(Files.readAllBytes(file.toPath()), FORMAT, VERSION);
      return NodeGraphCodec.fromJson(StateFormat.requireObject(json, "nodeGraph"));
    } catch (IOException | RuntimeException e) {
      logger.warn(
          "could not read {} ({}), starting with an empty graph; the file is overwritten by the"
              + " next save",
          file,
          e.toString());
      return new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    }
  }

  /**
   * Writes {@code graph} atomically. The in-memory encoding runs under {@code readLock} — the read
   * lock of the owning {@code NodeStore}, whose maintenance mutates the graph under the write lock
   * (REDPANDAJ-2DW) — while the file I/O and the fsync run outside it, so that a slow disk cannot
   * stall graph maintenance. Lock order: {@link #WRITE_LOCK} → read lock; nothing may call this
   * while holding the write lock.
   *
   * <p>A successful write also completes a pending migration: {@code settings} no longer has to
   * carry the embedded pre-T136 graph.
   *
   * @param readLock the graph's read lock, or {@code null} when no other thread can see the graph
   * @param settings the node's settings, or {@code null}
   * @return whether the file was written; a failure is logged, never thrown
   */
  static boolean save(
      int port,
      DefaultDirectedWeightedGraph<Node, NodeEdge> graph,
      Lock readLock,
      LocalSettings settings) {
    synchronized (WRITE_LOCK) {
      try {
        byte[] encoded = encode(graph, readLock);
        Files.createDirectories(Path.of(Settings.SAVE_DIR));
        StateFormat.writeAtomically(file(port), tmpFile(port), encoded);
      } catch (IOException | RuntimeException e) {
        // RuntimeException as well: the encoder throws unchecked (a vertex that is not a Node, a
        // ConcurrentModificationException, ...). The previous file stays as it was.
        logger.warn("error saving the node graph to {}", file(port), e);
        return false;
      }
    }
    if (settings != null) {
      settings.dropLegacyNodeGraph();
    }
    return true;
  }

  private static byte[] encode(DefaultDirectedWeightedGraph<Node, NodeEdge> graph, Lock readLock) {
    if (readLock != null) {
      readLock.lock();
    }
    try {
      JsonObject json = StateFormat.document(FORMAT, VERSION);
      json.add("nodeGraph", NodeGraphCodec.toJson(graph));
      return new Gson().toJson(json).getBytes(StandardCharsets.UTF_8);
    } finally {
      if (readLock != null) {
        readLock.unlock();
      }
    }
  }
}
