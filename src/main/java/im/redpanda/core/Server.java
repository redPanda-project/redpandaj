package im.redpanda.core;

import im.redpanda.jobs.KadRefreshJob;
import im.redpanda.jobs.NodeStoreMaintainJob;
import im.redpanda.jobs.PeerPerformanceTestSchedulerJob;
import im.redpanda.jobs.RequestPeerListJob;
import im.redpanda.store.NodeStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.security.Security;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {

    private static final Logger logger = LogManager.getLogger();

    public static final int VERSION = 22;
    static String MAGIC = "k3gV";
    public static LocalSettings localSettings;
    public static NodeId nodeId;
    public static KademliaId NONCE;
    public static boolean SHUTDOWN = false;
    public static int outBytes = 0;
    public static int inBytes = 0;
    public static ConnectionHandler connectionHandler;
    public static OutboundHandler outboundHandler;
    public static NodeStore nodeStore;
    public static ExecutorService threadPool = Executors.newFixedThreadPool(2);
    public static boolean startedUpSuccessful = false;

    public static SecureRandom secureRandom = new SecureRandom();
    public static Random random = new Random();

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    private final ServerContext serverContext;
    public PeerList peerList;

    public Server(ServerContext serverContext, ConnectionHandler connectionHandler) {
        this.serverContext = serverContext;
        Server.connectionHandler = connectionHandler;
        nodeStore = new NodeStore(serverContext);
        outboundHandler = new OutboundHandler(serverContext.getPeerList());
    }

    public static void triggerOutboundThread() {
        if (outboundHandler != null) {
            outboundHandler.tryInterrupt();
        }
    }

    public static void startedUpSuccessful(ServerContext serverContext) {
        localSettings = LocalSettings.load(serverContext.getPort());


        Settings.init(serverContext);
        ByteBufferPool.init();

        logger.debug("NodeStore has entries: " + nodeStore.size());

        nodeId = localSettings.getMyIdentity();
        NONCE = nodeId.getKademliaId();
        logger.info("started node with KademliaId: " + NONCE.toString() + " port: " + serverContext.getPort());


        outboundHandler.init();


        new HTTPServer().start();

        startedUpSuccessful = true;

        new PeerPerformanceTestSchedulerJob(serverContext).start();
        new RequestPeerListJob(serverContext).start();
        new NodeStoreMaintainJob(serverContext).start();

    }

    public static void shutdown(ServerContext serverContext) {
        Server.SHUTDOWN = true;

//        Server.nodeStore.saveToDisk();

//        KadStoreManager.maintain();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Server.nodeStore.close();
        Server.localSettings.save(serverContext.getPort());
    }

    public void start() {
        connectionHandler.start();
        //this is a permanent job and will run every hour...
        new KadRefreshJob(serverContext).start();
    }


}
