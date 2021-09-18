package im.redpanda.core;

import im.redpanda.jobs.KadRefreshJob;
import im.redpanda.jobs.NodeStoreMaintainJob;
import im.redpanda.jobs.PeerPerformanceTestSchedulerJob;
import im.redpanda.jobs.RequestPeerListJob;
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
    public final static String MAGIC = "k3gV";
    public static boolean shuttingDown = false;
    public static int outBytes = 0;
    public static int inBytes = 0;
    private ConnectionHandler connectionHandler;
    public static OutboundHandler outboundHandler;
    public static ExecutorService threadPool = Executors.newFixedThreadPool(2);

    public static final SecureRandom secureRandom = new SecureRandom();

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    private final ServerContext serverContext;
    public PeerList peerList;

    public Server(ServerContext serverContext, ConnectionHandler connectionHandler) {
        this.serverContext = serverContext;
        this.connectionHandler = connectionHandler;
        outboundHandler = new OutboundHandler(serverContext);
    }

    public static void triggerOutboundThread() {
        if (outboundHandler != null) {
            outboundHandler.tryInterrupt();
        }
    }

    public static void startUpRoutines(ServerContext serverContext) {
        Settings.init(serverContext);


        new HTTPServer(serverContext).start();

        outboundHandler.start();

        new PeerPerformanceTestSchedulerJob(serverContext).start();
        new RequestPeerListJob(serverContext).start();
        new NodeStoreMaintainJob(serverContext).start();

    }

    public static void shutdown(ServerContext serverContext) {
        Server.shuttingDown = true;

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

        serverContext.getNodeStore().close();
        serverContext.getLocalSettings().save(serverContext.getPort());
    }

    public void start() {
        connectionHandler.start();
    }


}
