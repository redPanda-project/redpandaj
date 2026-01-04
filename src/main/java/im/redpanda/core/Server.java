package im.redpanda.core;

import im.redpanda.jobs.NodeStoreMaintainJob;
import im.redpanda.jobs.PeerPerformanceTestSchedulerJob;
import im.redpanda.jobs.RequestPeerListJob;
import java.security.SecureRandom;
import java.security.Security;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {

  public static final int VERSION = 22;
  public static final String MAGIC = "k3gV";
  public static boolean shuttingDown = false;
  public static int outBytes = 0;
  public static int inBytes = 0;
  private ConnectionHandler connectionHandler;
  public static OutboundHandler outboundHandler;
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
    HashMap<KademliaId, Peer> peers = Saver.loadPeers();
    for (Peer p : peers.values()) {
      serverContext.getPeerList().add(p);
    }
    System.out.println("Restored " + peers.size() + " peers from disk");

    new PeerPerformanceTestSchedulerJob(serverContext).start();
    new RequestPeerListJob(serverContext).start();
    new NodeStoreMaintainJob(serverContext).start();
  }

  public static void shutdown(ServerContext serverContext) {
    Server.shuttingDown = true;

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace(); // NOSONAR (java:S4507): intentional fallback console output during
      // shutdown;
      // replace with logger later
      Thread.currentThread().interrupt();
    }

    Saver.savePeers(serverContext.getPeerList().getPeerArrayList());
    serverContext.getNodeStore().close();
    serverContext.getLocalSettings().save(serverContext.getPort());
  }

  public void start() {
    connectionHandler.start();
  }
}
