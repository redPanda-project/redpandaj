package im.redpanda.core;

import java.security.Security;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Server {

    public static final int VERSION = 21;
    public static int MY_PORT = -1;
    static String MAGIC = "k3gV";
    public static NodeId nodeId;
    public static KademliaId NONCE;
    public static boolean SHUTDOWN = false;
    public static ArrayList<Peer> peerList = null;
    public static ReentrantReadWriteLock peerListLock = new ReentrantReadWriteLock();
    public static int outBytes = 0;
    public static int inBytes = 0;
    public static ConnectionHandler connectionHandler;
    public static OutboundHandler outboundHandler;
    public static ArrayList<Peer>[] buckets = new ArrayList[KademliaId.ID_LENGTH];
    public static ArrayList<Peer>[] bucketsReplacement = new ArrayList[KademliaId.ID_LENGTH];


    static {

        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

        peerList = Saver.loadPeers();

        nodeId = new NodeId();
        NONCE = nodeId.getKademliaId();

        //init all buckets!
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new ArrayList<>(Settings.k);
            bucketsReplacement[i] = new ArrayList<>();
        }

    }

    public static void start() {


        connectionHandler = new ConnectionHandler();
        connectionHandler.start();


        outboundHandler = new OutboundHandler();
        outboundHandler.init();

    }


    public static void triggerOutboundthread() {
        if (outboundHandler != null) {
            outboundHandler.tryInterrupt();
        }
    }


    public static void addPeerToBucket(Peer p) {
        peerListLock.writeLock().lock();
        try {
            int distanceToUs = p.getNodeId().getDistanceToUs();
            buckets[distanceToUs - 1].add(p);
        } finally {
            peerListLock.writeLock().unlock();
        }
    }

    public static void removePeerFromBucket(Peer p) {
        peerListLock.writeLock().lock();
        try {
            int distanceToUs = p.getNodeId().getDistanceToUs();
            buckets[distanceToUs - 1].remove(p);
        } finally {
            peerListLock.writeLock().unlock();
        }
    }


    public static Peer findPeer(Peer peer) {

        peerListLock.writeLock().lock();
        try {
            for (Peer p : peerList) {

                if (p.equalsIpAndPort(peer)) {
                    return p;
                }

            }


            peerList.add(peer);


        } finally {
            peerListLock.writeLock().unlock();
        }
        return peer;
    }

    public static void removePeer(Peer peer) {
        peerListLock.writeLock().lock();
        peerList.remove(peer);
        peer.removeNodeId();
        peerListLock.writeLock().unlock();
    }
}
