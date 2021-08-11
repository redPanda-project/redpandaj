package im.redpanda.core;

import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class stores all peers in two Hashmaps for fast get operations using the {@link KademliaId} and Ip+Port.
 * For the connections we establish, we need a sorted List with regard to specific parameters.
 * This class maintains an ArrayList with the same peers as in the Hashmaps.
 * In addition, a peer can be optionally be stored in the DHT routing table, called the Buckets.
 * Note that not all nodes will be in the routing table (Buckets).
 * <p>
 * <b>Warning</b>: Do not change any of the required data ({@link KademliaId}, Ip, Port) of a {@link Peer} if
 * it is present in the Peerlist without updating the corresponding List/HashMap.
 */
public class PeerList {

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger();

    /**
     * We store each Peer in a hashmap for fast get operations via KademliaId
     */
    private final HashMap<KademliaId, Peer> peerlist;

    /**
     * We store each Peer in a hashmap for fast get operations via Ip and Port
     */
    private final HashMap<Integer, Peer> peerlistIpPort;

    /**
     * Blacklist of ips via HashMap
     */
    private final HashMap<Integer, Peer> blacklistIp;

    /**
     * We store each Peer in a ArrayList to obtain a sorted list of Peers where the good peers are on top
     */
    private final ArrayList<Peer> peerArrayList;

    /**
     * ReadWriteLock for peerlist peerArrayList and Buckets
     */
    private final ReadWriteLock readWriteLock;

    /**
     * Buckets for the Kademlia routing
     */
    private final ArrayList<Peer>[] buckets;
    private final ArrayList<Peer>[] bucketsReplacement;


    public PeerList() {
        peerlist = new HashMap<>();
        peerlistIpPort = new HashMap<>();
        blacklistIp = new HashMap<>();
        peerArrayList = new ArrayList<>();
        readWriteLock = new ReentrantReadWriteLock();
        buckets = new ArrayList[KademliaId.ID_LENGTH];
        bucketsReplacement = new ArrayList[KademliaId.ID_LENGTH];
    }

    /**
     * Adds a Peer to the Peerlist by handling all Hashmaps and the Arraylist.
     * Acquires locks.
     * KademliaId is optional, ip and port have to be known of the Peer.
     *
     * @param peer The peer to add to the PeerList.
     * @return old peer, null if no old peer or old peer null.
     */
    public Peer add(Peer peer) {
        Peer oldPeer = null;

        // we have to check if the peer is already in the PeerList, for this we use the HashMaps since they are much faster
        if (peer.getKademliaId() != null) {
            oldPeer = peerlist.get(peer.getKademliaId());
            if (oldPeer != null) {
                // Peer with same KademliaId exists already
                Log.put("Peer with same KademliaId exists already", 100);
                return oldPeer;
            }
        }

        /**
         * We allow peers without connection details (ip,port) in the PeerList, since after a wipe of data the new Node
         * has the same (ip,port) but different Identity. The (ip,port) will then be removed from the old Peer.
         * Since we allow Peers without (ip,port) in general we allow to add Peers without (ip,port) here.
         */
        if (peer.getIp() != null) {
            oldPeer = peerlistIpPort.get(getIpPortHash(peer));
            if (oldPeer != null) {
                // Peer with same Ip+Port exists already
                System.out.println("Peer with same Ip+Port exists already");
                return oldPeer;
            }
        }

        try {
            readWriteLock.writeLock().lock();
            if (peer.getKademliaId() != null) {
                oldPeer = peerlist.put(peer.getKademliaId(), peer);
            }
            peerlistIpPort.put(getIpPortHash(peer), peer);
            peerArrayList.add(peer);
        } finally {
            readWriteLock.writeLock().unlock();
        }
        return oldPeer;
    }

    /**
     * Hash method for the peerlistIpPort list.
     *
     * @param peer
     * @return hash value
     */
    private Integer getIpPortHash(Peer peer) {
        return getIpPortHash(peer.getIp(), peer.getPort());
    }

    private static Integer getIpPortHash(String ip, int port) {
        //ToDo: we need later a better method
        return ip.hashCode() + port;
    }

    /**
     * Removes a {@link Peer} from the PeerList.
     * Removes the Peer from both Hashmaps and the ArrayList
     *
     * @param peer
     */
    public boolean remove(Peer peer) {
//        System.out.println("remove peer: " + peer.getKademliaId());
        readWriteLock.writeLock().lock();
        try {
            if (peer.getKademliaId() == null) {
                return removeByObject(peer);
            }
            return remove(peer.getKademliaId());
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private boolean removeByObject(Peer peer) {
        readWriteLock.writeLock().lock();
        try {
            boolean removed = peerArrayList.remove(peer);
            if (!removed) {
                return false;
            }
            if (peer.getIp() != null && peer.getPort() != 0) {
                peerlistIpPort.remove(getIpPortHash(peer));
            }
            return true;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Completely removes the Peer from all Lists by Ip and Port.
     *
     * @param ip
     * @param port
     * @return
     */
    public boolean removeIpPort(String ip, int port) {
        logger.info("remove ipport: " + ip + ":" + port);
        readWriteLock.writeLock().lock();
        try {
            Peer peer = peerlistIpPort.remove(getIpPortHash(ip, port));
            if (peer == null) {
                return false;
            }
            peerlist.remove(peer.getKademliaId());
            peerArrayList.remove(peer);
            return true;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Removes the Peer from the IpPortList, peer is still in the other lists. Use this only for ip,port changes.
     *
     * @param ip
     * @param port
     * @return
     */
    public boolean removeIpPortOnly(String ip, int port) {
        readWriteLock.writeLock().lock();
        try {
            Peer peer = peerlistIpPort.remove(getIpPortHash(ip, port));
            return peer != null;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Removes a {@link Peer} by providing a {@link KademliaId} from the PeerList.
     * Removes the Peer from both Hashmaps and the ArrayList
     *
     * @param id
     */
    public boolean remove(KademliaId id) {
        boolean removedOnePeer = false;
        try {
            readWriteLock.writeLock().lock();
            Peer remove = peerlist.remove(id);
            if (remove == null) {
                return false;
            }
            removedOnePeer = peerArrayList.remove(remove);
            if (remove.getIp() != null && remove.getPort() != 0) {
                peerlistIpPort.remove(getIpPortHash(remove));
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
        return removedOnePeer;
    }

    /**
     * clears all underlying lists and Hashmaps. Does not acquire locks.
     */
    public void clear() {
        peerlist.clear();
        peerArrayList.clear();
        peerlistIpPort.clear();
    }

    public Peer get(KademliaId id) {
        try {
            readWriteLock.readLock().lock();
            return peerlist.get(id);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public ReadWriteLock getReadWriteLock() {
        return readWriteLock;
    }

    public ArrayList<Peer> getPeerArrayList() {
        return peerArrayList;
    }


    /**
     * Returns the size of the ArrayList which should contain all Peers.
     *
     * @return
     */
    public int size() {
        readWriteLock.readLock().lock();
        try {
            return peerArrayList.size();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * Call this method to update an identity/KademliaId of a Peer.
     *
     * @param peer
     */
    public void updateKademliaId(Peer peer, KademliaId newId) {

        KademliaId oldId = peer.getKademliaId();
        System.out.println("updating KadId, old " + oldId + " new: " + newId.toString());

        readWriteLock.writeLock().lock();
        try {
            if (oldId != null) {
                // We have to remove the old id
                peerlist.remove(oldId);
            }
            peer.setNodeId(new NodeId(newId));
            peerlist.put(newId, peer);
        } finally {
            readWriteLock.writeLock().unlock();
        }

    }

    public Peer getGoodPeer() {
        return getGoodPeer(0.4f);
    }

    public Peer getGoodPeer(float upperPercent) {
        readWriteLock.writeLock().lock();
        try {
            Collections.sort(peerArrayList);

            int size = peerArrayList.size();

            if (size == 0) {
                return null;
            }

            //lets get a random x percent peer

            int max = (int) Math.ceil(size * upperPercent);

            int i = Server.random.nextInt(max);

            return peerArrayList.get(i);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void clearConnectionDetails(Peer peer) {
        Log.put("clearing peer: " + peer.getIp() + ":" + peer.getPort(), 50);
        removeIpPortOnly(peer.getIp(), peer.getPort());
        peer.removeIpAndPort();
    }

}
