package im.redpanda.core;

import im.redpanda.jobs.NodeStoreMaintainJob;
import im.redpanda.jobs.PeerPerformanceTestSchedulerJob;
import im.redpanda.jobs.RequestPeerListJob;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {

  public static final int VERSION = 22;
  public static final String MAGIC = "k3gV";
  private static volatile boolean shuttingDown = false;
  private static final AtomicInteger outBytes = new AtomicInteger(0);
  private static final AtomicInteger inBytes = new AtomicInteger(0);
  private ConnectionHandler connectionHandler;
  private static OutboundHandler outboundHandler;
  private static final Logger log = LoggerFactory.getLogger(Server.class);
  public static final ExecutorService threadPool = Executors.newVirtualThreadPerTaskExecutor();

  public static final SecureRandom secureRandom = new SecureRandom();

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  public Server(ServerContext serverContext, ConnectionHandler connectionHandler) {
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

    // restore peers
    Map<KademliaId, Peer> peers = Saver.loadPeers();
    for (Peer p : peers.values()) {
      serverContext.getPeerList().add(p);
    }
    log.info("Restored {} peers from disk", peers.size());

    new PeerPerformanceTestSchedulerJob(serverContext).start();
    new RequestPeerListJob(serverContext).start();
    new NodeStoreMaintainJob(serverContext).start();
  }

  public static void shutdown(ServerContext serverContext) {
    Server.shuttingDown = true;

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      log.warn("Interrupted during shutdown", e);
      Thread.currentThread().interrupt();
    }

    Saver.savePeers(serverContext.getPeerList().getPeerArrayList());
    serverContext.getNodeStore().close();
    serverContext.getLocalSettings().save(serverContext.getPort());
  }

  public void start() {
    connectionHandler.start();
  }

  public static boolean isShuttingDown() {
    return shuttingDown;
  }

  public static void setShuttingDown(boolean shuttingDown) {
    Server.shuttingDown = shuttingDown;
  }

  public static int getOutBytes() {
    return outBytes.get();
  }

  public static void addOutBytes(int bytes) {
    outBytes.addAndGet(bytes);
  }

  public static int getInBytes() {
    return inBytes.get();
  }

  public static void addInBytes(int bytes) {
    inBytes.addAndGet(bytes);
  }

  public static OutboundHandler getOutboundHandler() {
    return outboundHandler;
  }

  public static void setOutboundHandler(OutboundHandler outboundHandler) {
    Server.outboundHandler = outboundHandler;
  }
}
