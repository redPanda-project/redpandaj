package im.redpanda.core;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Saver {

    public static final String SAVE_DIR = "data";

    public static void savePeerss(ArrayList<Peer> peers) {
        ArrayList<PeerSaveable> arrayList = new ArrayList<PeerSaveable>();

        for (Peer peer : peers) {
            arrayList.add(peer.toSaveable());
        }
        //arrayList = (ArrayList<PeerSaveable>) arrayList.clone();//hack?

        FileOutputStream fileOutputStream = null;
        ObjectOutputStream objectOutputStream = null;

        try {
            File file = new File(SAVE_DIR + "/peers.dat");

            file.createNewFile();
            fileOutputStream = new FileOutputStream(file);
            objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(arrayList);
            objectOutputStream.close();
            fileOutputStream.close();

        } catch (IOException ex) {
            Logger.getLogger(Saver.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (objectOutputStream != null) {
                    objectOutputStream.close();
                }
            } catch (IOException ex) {
                Logger.getLogger(Saver.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                if (fileOutputStream != null) {
                    try {
                        fileOutputStream.close();
                    } catch (IOException ex) {
                        Logger.getLogger(Saver.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }

    }

    public static HashMap<KademliaId, Peer> loadPeers() {
        try {
            File file = new File(SAVE_DIR + "/peers.dat");

            FileInputStream fileInputStream = new FileInputStream(file);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            Object readObject = objectInputStream.readObject();
            objectInputStream.close();
            fileInputStream.close();

            ArrayList<PeerSaveable> pp = (ArrayList<PeerSaveable>) readObject;
            HashMap<KademliaId, Peer> arrayList = new HashMap<KademliaId, Peer>();


            for (PeerSaveable p : pp) {
                arrayList.put(p.nonce, p.toPeer());
            }


            return arrayList;


        } catch (ClassNotFoundException ex) {
        } catch (IOException ex) {
        } catch (ClassCastException ex) {
        }

        System.out.println("could not load peers.dat");

        return new HashMap<KademliaId, Peer>();
    }

}
