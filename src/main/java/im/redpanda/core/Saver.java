package im.redpanda.core;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Saver {

  private Saver() {}

  public static final String SAVE_DIR = "data";

  public static void savePeers(List<Peer> peers) {
    ArrayList<PeerSaveable> arrayList = new ArrayList<>();

    for (Peer peer : peers) {
      if (peer.getNodeId() != null) {
        arrayList.add(peer.toSaveable());
      }
    }

    File mkdirs = new File(SAVE_DIR);
    mkdirs.mkdir();

    File file = new File(SAVE_DIR + "/peers.dat");

    try {
      if (file.createNewFile()) {
        log.info("Created new peers.dat file");
      }
      try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
          objectOutputStream.writeObject(arrayList);
        }
      }

    } catch (IOException ex) {
      log.error("Could not save peers", ex);
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
