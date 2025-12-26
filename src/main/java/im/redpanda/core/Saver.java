package im.redpanda.core;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Saver {

  private Saver() {}

  public static final String SAVE_DIR = "data";

  public static void savePeerss(ArrayList<Peer> peers) {
    ArrayList<PeerSaveable> arrayList = new ArrayList<PeerSaveable>();

    for (Peer peer : peers) {
      arrayList.add(peer.toSaveable());
    }
    // arrayList = (ArrayList<PeerSaveable>) arrayList.clone();//hack?

    File file = new File(SAVE_DIR + "/peers.dat");

    try {
      file.createNewFile();
      try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
          objectOutputStream.writeObject(arrayList);
        }
      }

    } catch (IOException ex) {
      Logger.getLogger(Saver.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public static HashMap<KademliaId, Peer> loadPeers() {
    try {
      File file = new File(SAVE_DIR + "/peers.dat");

      try (FileInputStream fileInputStream = new FileInputStream(file)) {
        try (ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
          Object readObject = objectInputStream.readObject();

          @SuppressWarnings("unchecked")
          ArrayList<PeerSaveable> pp = (ArrayList<PeerSaveable>) readObject;
          HashMap<KademliaId, Peer> hashMap = new HashMap<KademliaId, Peer>();

          for (PeerSaveable p : pp) {
            hashMap.put(p.nodeId.getKademliaId(), p.toPeer());
          }
          return hashMap;
        }
      }

    } catch (ClassNotFoundException | IOException | ClassCastException ex) {
      // we can ignore the errors here and just reseed the peers
    }

    System.out.println("could not load peers.dat");

    return new HashMap<KademliaId, Peer>();
  }
}
