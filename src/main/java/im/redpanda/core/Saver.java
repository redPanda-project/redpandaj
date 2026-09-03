package im.redpanda.core;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Persistence of the dialable peer list ({@code data/peers.json}).
 *
 * <p>T117: explicit JSON instead of a Java object stream. The old format pinned the fully qualified
 * names of {@code PeerSaveable}, {@code NodeId} and {@code KademliaId} into the file, which is what
 * made the package moves of T118 a state-destroying change (DDD review §5).
 *
 * <p><b>No migration path</b> (user decision 2026-09-01): the pre-T117 {@code data/peers.dat} is
 * neither read nor deleted — a node that finds only that file starts with an empty peer list and
 * bootstraps from {@code REDPANDA_KNOWN_NODES}. Losing the file costs nothing but the first
 * reconnect.
 */
@Slf4j
public class Saver {

  private Saver() {}

  public static final String SAVE_DIR = "data";

  /** Header {@code format} of the peers file. */
  static final String FORMAT = "redpanda-peers";

  /** Header {@code version} of the peers file. */
  static final int VERSION = 1;

  /**
   * Guards the write to {@code peers.json}. The recurring {@code SaveJobs} and {@code
   * Server.shutdown} can both be in {@link #savePeers(List)} at the same time, and they would race
   * on the shared temporary file and its rename. Mirrors the {@code synchronized} on {@code
   * LocalSettings.save(int)}.
   */
  private static final Object SAVE_LOCK = new Object();

  /**
   * Snapshots the peer list under its read lock and persists the snapshot.
   *
   * <p>The callers used to hand the live list — {@code PeerList.getPeerArrayList()}, removed in
   * T115 — straight to {@link #savePeers(List)}, which iterates it. Network threads add and remove
   * peers concurrently, so the iteration could throw a {@code ConcurrentModificationException} and
   * the save was lost (the same class of bug as redpandaj#260 and REDPANDAJ-2DZ; every other
   * iteration site takes the lock). Only the snapshot is taken under the lock, the encoding and the
   * file I/O run without it.
   */
  public static void savePeers(PeerList peerList) {
    savePeers(peerList.snapshot());
  }

  /**
   * Persists the given peers. Callers holding a {@link PeerList} must use {@link
   * #savePeers(PeerList)} instead — this overload assumes the list is not mutated concurrently.
   *
   * <p>Written via a temporary file plus fsync and an atomic rename, for the same reason as {@code
   * LocalSettings.save()}: writing in place truncates the file first, so a failure half way through
   * leaves a corrupt file behind. That is less severe here (an unreadable peers file only costs the
   * known peers, {@link #loadPeers()} falls back to an empty map) but the pattern is three lines,
   * so there is no reason to keep the truncating write. The write is serialized on {@link
   * #SAVE_LOCK} so that two savers cannot race on the shared temporary file.
   */
  public static void savePeers(List<Peer> peers) {
    JsonArray peersJson = new JsonArray();

    for (Peer peer : peers) {
      // Bootstrap peers may have a NodeId with a known KademliaId but no verify key yet
      // (handshake not completed) - skip those, they cannot be encoded and are not restorable.
      if (peer.getNodeId() == null || !peer.getNodeId().hasKey()) {
        continue;
      }
      // Only dialable peers are worth restoring: the whole point of the peers file is to have
      // addresses to connect to on the next start. Inbound-only entries (a light client announces
      // port 0, see Peer#isDialable) cannot be dialled by anyone, and persisting them is what let
      // them survive restarts and be re-gossiped afterwards - the affected node's peers.dat held 82
      // loopback entries alone (T86).
      if (!peer.isDialable()) {
        continue;
      }
      PeerSaveable saveable = peer.toSaveable();
      JsonObject peerJson = new JsonObject();
      peerJson.addProperty("ip", saveable.ip);
      peerJson.addProperty("port", saveable.port);
      peerJson.add("nodeId", NodeIdCodec.nodeIdToJson(saveable.nodeId));
      peerJson.addProperty("retries", saveable.retries);
      peersJson.add(peerJson);
    }

    JsonObject document = StateFormat.document(FORMAT, VERSION);
    document.add("peers", peersJson);

    File mkdirs = new File(SAVE_DIR);
    mkdirs.mkdir();

    synchronized (SAVE_LOCK) {
      try {
        StateFormat.writeAtomically(
            peersFile(),
            tmpPeersFile(),
            new Gson().toJson(document).getBytes(StandardCharsets.UTF_8));
      } catch (IOException | RuntimeException ex) {
        log.error("Could not save peers", ex);
      }
    }
  }

  /**
   * Loads the dialable peers, or an empty map if there is none, the file is unreadable, or only the
   * pre-T117 {@code peers.dat} exists (never migrated, never deleted — see the class javadoc).
   */
  public static Map<KademliaId, Peer> loadPeers() {
    File file = peersFile();

    if (!file.exists()) {
      File legacy = legacyPeersFile();
      if (legacy.exists()) {
        log.info(
            "ignoring the pre-T117 Java-serialized peer list {}: it is no longer read and can be"
                + " deleted; bootstrapping from the known nodes instead",
            legacy);
      }
      return new HashMap<>();
    }

    try {
      JsonObject document = StateFormat.parse(Files.readAllBytes(file.toPath()), FORMAT, VERSION);
      JsonElement peersJson = document.get("peers");
      if (peersJson == null || !peersJson.isJsonArray()) {
        throw new IOException("missing array member 'peers'");
      }

      HashMap<KademliaId, Peer> loaded = new HashMap<>();
      for (JsonElement element : peersJson.getAsJsonArray()) {
        if (!element.isJsonObject()) {
          throw new IOException("peers must hold objects");
        }
        JsonObject peerJson = element.getAsJsonObject();
        JsonElement ip = peerJson.get("ip");
        if (ip == null || !ip.isJsonPrimitive()) {
          throw new IOException("peer without an ip");
        }
        // savePeers writes only dialable peers (Peer#isDialable) with a non-negative retry count,
        // so anything else means the file is corrupt or tampered with - and the point of this file
        // is addresses to dial. Loading such an entry would re-introduce exactly the undialable
        // peers T86 removed, so the whole file is rejected instead.
        int port = StateFormat.optInt(peerJson, "port", 0);
        if (port <= 0 || port > 65535) {
          throw new IOException("peer with an undialable port: " + port);
        }
        int retries = StateFormat.optInt(peerJson, "retries", 0);
        if (retries < 0) {
          throw new IOException("peer with a negative retry count: " + retries);
        }
        PeerSaveable saveable =
            new PeerSaveable(
                ip.getAsString(),
                port,
                NodeIdCodec.nodeIdFromJson(StateFormat.requireObject(peerJson, "nodeId")),
                retries);
        loaded.put(saveable.nodeId.getKademliaId(), saveable.toPeer());
      }
      return loaded;

    } catch (IOException | RuntimeException ex) {
      log.warn(
          "could not read {} ({}) - starting with an empty peer list, the file is kept",
          file,
          ex.toString());
    }

    return new HashMap<>();
  }

  static File peersFile() {
    return new File(SAVE_DIR + "/peers.json");
  }

  static File tmpPeersFile() {
    return new File(SAVE_DIR + "/peers.json.tmp");
  }

  /** The pre-T117 Java-serialized peer list. Never read, never deleted — only reported. */
  static File legacyPeersFile() {
    return new File(SAVE_DIR + "/peers.dat");
  }
}
