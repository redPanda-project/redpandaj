package im.redpanda.core;

import im.redpanda.jobs.KadRefreshJob;
import im.redpanda.kademlia.KadContent;
import im.redpanda.kademlia.KadStoreManager;
import im.redpanda.store.NodeStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.security.Security;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Server {

    private static final Logger logger = LogManager.getLogger();

    public static final int VERSION = 22;
    public static int MY_PORT = -1;
    static String MAGIC = "k3gV";
    public static LocalSettings localSettings;
    public static NodeId nodeId;
    public static KademliaId NONCE;
    public static boolean SHUTDOWN = false;
    public static ReadWriteLock peerListLock;
    public static int outBytes = 0;
    public static int inBytes = 0;
    public static ConnectionHandler connectionHandler;
    public static OutboundHandler outboundHandler;
    public static ArrayList<Peer>[] buckets = new ArrayList[KademliaId.ID_LENGTH];
    public static ArrayList<Peer>[] bucketsReplacement = new ArrayList[KademliaId.ID_LENGTH];
    public static NodeStore nodeStore;
    public static ExecutorService threadPool = Executors.newFixedThreadPool(2);
    public static boolean startedUpSuccessful = false;

    public static SecureRandom secureRandom = new SecureRandom();
    public static Random random = new Random();


    static {

        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

//        peerList = Saver.loadPeers();
        peerListLock = PeerList.getReadWriteLock();

        //init all buckets!
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new ArrayList<>(Settings.k);
            bucketsReplacement[i] = new ArrayList<>();
        }

    }

    public static void start() {

        connectionHandler = new ConnectionHandler(true);
        connectionHandler.start();

        //this is a permanent job and will run every hour...
        new KadRefreshJob().start();

    }


    public static void triggerOutboundthread() {
        if (outboundHandler != null) {
            outboundHandler.tryInterrupt();
        }
    }


    public static void addPeerToBucket(Peer p) {
        peerListLock.writeLock().lock();
        try {
            int distanceToUs = p.getKademliaId().getDistanceToUs();
            buckets[distanceToUs - 1].add(p);
        } finally {

            peerListLock.writeLock().unlock();
        }
    }

    public static void removePeerFromBucket(Peer p) {
        peerListLock.writeLock().lock();
        try {
            int distanceToUs = p.getKademliaId().getDistanceToUs();
            buckets[distanceToUs - 1].remove(p);
        } finally {
            peerListLock.writeLock().unlock();
        }
    }


    public static Peer findPeer(Peer peer) {

//        peerListLock.writeLock().lock();
//        try {
//            for (Peer p : peerList.values()) {
//
//                if (p.equalsIpAndPort(peer)) {
//                    return p;
//                }
//
//            }
//
//
//            peerList.put(peer.getKademliaId(),peer);
//
//
//        } finally {
//            peerListLock.writeLock().unlock();
//        }
//        return peer;

        return null;
    }

    public static void removePeer(Peer peer) {
        peerListLock.writeLock().lock();
        PeerList.remove(peer);
        peer.removeNodeId();
        peerListLock.writeLock().unlock();
    }

    public static void startedUpSuccessful() {
        localSettings = LocalSettings.load(Server.MY_PORT);
        nodeStore = new NodeStore();

        Settings.init();
        ByteBufferPool.init();

        logger.debug("NodeStore has entries: " + nodeStore.size());

        nodeId = localSettings.getMyIdentity();
        NONCE = nodeId.getKademliaId();
        logger.info("started node with KademliaId: " + NONCE.toString() + " port: " + Server.MY_PORT);

        outboundHandler = new OutboundHandler();
        outboundHandler.init();

        PeerJobs.startUp();

        new HTTPServer().start();

        startedUpSuccessful = true;
    }

    public static void shutdown() {
        Server.SHUTDOWN = true;

//        Server.nodeStore.saveToDisk();

//        KadStoreManager.maintain();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Server.nodeStore.close();
        Server.localSettings.save(Server.MY_PORT);
    }
}
