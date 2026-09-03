package im.redpanda.core;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Saver {

  private Saver() {}

  public static final String SAVE_DIR = "data";

  /**
   * Guards the write to {@code peers.dat}. The recurring {@code SaveJobs} and {@code
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
   * iteration site takes the lock). Only the snapshot is taken under the lock, the serialization
   * and the file I/O run without it.
   */
  public static void savePeers(PeerList peerList) {
    savePeers(peerList.snapshot());
  }

  /**
   * Persists the given peers. Callers holding a {@link PeerList} must use {@link
   * #savePeers(PeerList)} instead — this overload assumes the list is not mutated concurrently.
   *
   * <p>Written via a temporary file plus fsync and an atomic rename, for the same reason as {@code
   * LocalSettings.save()}: writing in place truncates {@code peers.dat} first, so a failure half
   * way through leaves a corrupt file behind. That is less severe here (an unreadable {@code
   * peers.dat} only costs the known peers, {@link #loadPeers()} falls back to an empty map) but the
   * pattern is three lines, so there is no reason to keep the truncating write. The write is
   * serialized on {@link #SAVE_LOCK} so that two savers cannot race on the shared temporary file.
   */
  public static void savePeers(List<Peer> peers) {
    ArrayList<PeerSaveable> arrayList = new ArrayList<>();

    for (Peer peer : peers) {
      // Bootstrap peers may have a NodeId with a known KademliaId but no verify key yet
      // (handshake not completed) - skip those, serializing them would NPE in NodeId#writeObject.
      if (peer.getNodeId() == null || !peer.getNodeId().hasKey()) {
        continue;
      }
      // Only dialable peers are worth restoring: the whole point of peers.dat is to have addresses
      // to connect to on the next start. Inbound-only entries (a light client announces port 0,
      // see Peer#isDialable) cannot be dialled by anyone, and persisting them is what let them
      // survive restarts and be re-gossiped afterwards - the affected node's peers.dat held 82
      // loopback entries alone (T86).
      if (!peer.isDialable()) {
        continue;
      }
      arrayList.add(peer.toSaveable());
    }

    File mkdirs = new File(SAVE_DIR);
    mkdirs.mkdir();

    File file = new File(SAVE_DIR + "/peers.dat");
    File tmpFile = new File(SAVE_DIR + "/peers.dat.tmp");

    synchronized (SAVE_LOCK) {
      try {
        try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
          objectOutputStream.writeObject(arrayList);
          objectOutputStream.flush();
          fileOutputStream.getFD().sync();
        }

        Files.move(
            tmpFile.toPath(),
            file.toPath(),
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE);

      } catch (IOException ex) {
        log.error("Could not save peers", ex);
      } finally {
        if (tmpFile.exists() && !tmpFile.delete()) {
          log.info("could not delete temporary peers file {}", tmpFile);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<KademliaId, Peer> loadPeers() {
    try {
      File file = new File(SAVE_DIR + "/peers.dat");

      if (!file.exists()) {
        return new HashMap<>();
      }

      try (FileInputStream fileInputStream = new FileInputStream(file)) {
        try (ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
          Object readObject = objectInputStream.readObject();

          ArrayList<PeerSaveable> pp = (ArrayList<PeerSaveable>) readObject;
          HashMap<KademliaId, Peer> hashMap = new HashMap<>();

          for (PeerSaveable p : pp) {
            hashMap.put(p.nodeId.getKademliaId(), p.toPeer());
          }
          return hashMap;
        }
      }

    } catch (ClassNotFoundException | IOException | ClassCastException ex) {
      log.error("Could not load peers", ex);
    }

    log.info("could not load peers.dat");

    return new HashMap<>();
  }
}
