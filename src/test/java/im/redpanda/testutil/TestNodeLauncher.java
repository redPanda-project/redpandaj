package im.redpanda.testutil;

import im.redpanda.core.LocalSettings;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import im.redpanda.dht.KadRefreshJob;
import im.redpanda.mailbox.OutboundCleanupJob;
import im.redpanda.mailbox.OutboundService;
import im.redpanda.mailbox.OutboundStore;
import im.redpanda.ops.JobScheduler;
import im.redpanda.ops.Log;
import im.redpanda.ops.SaveJobs;
import im.redpanda.ops.ServerRestartJob;
import im.redpanda.ops.Settings;
import im.redpanda.ops.UpTimeReporterJob;
import im.redpanda.routing.GMManagerCleanJobs;
import im.redpanda.routing.PeerPerformanceTestSchedulerJob;
import im.redpanda.routing.graph.Node;
import im.redpanda.routing.graph.NodeConnectionPointsSeenJob;
import im.redpanda.routing.graph.NodeInfoSetRefreshJob;
import im.redpanda.routing.graph.NodeStore;
import im.redpanda.routing.graph.NodeStoreMaintainJob;
import im.redpanda.transport.ByteBufferPool;
import im.redpanda.transport.ConnectionHandler;
import im.redpanda.transport.OutboundHandler;
import im.redpanda.transport.PeerJobs;
import im.redpanda.transport.RequestPeerListJob;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

/**
 * Lightweight launcher used only from E2E tests. It mirrors the application boot sequence without
 * the interactive console and accepts a simple "stop" command on stdin to trigger a graceful
 * shutdown.
 */
public final class TestNodeLauncher {

  private static final AtomicBoolean shuttingDown = new AtomicBoolean(false);

  private TestNodeLauncher() {}

  public static void main(String[] args) throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    Path workDir = Path.of(System.getProperty("redpanda.workdir", ".")).toAbsolutePath();
    Files.createDirectories(workDir);
    System.setProperty("user.dir", workDir.toString());

    configureSettings();

    ServerContext serverContext = new ServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, true);
    serverContext.setConnectionHandler(connectionHandler);
    int port = connectionHandler.bind();
    serverContext.setPort(port);
    serverContext.setLocalSettings(LocalSettings.load(port));
    serverContext.setNodeId(serverContext.getLocalSettings().getMyIdentity());
    serverContext.setNodeStore(NodeStore.buildWithDiskCache(serverContext));

    // Outbound Service V1 Init
    OutboundStore outboundStore = OutboundStore.forContext(serverContext);
    serverContext.setOutboundStore(outboundStore);
    serverContext.setOutboundService(new OutboundService(outboundStore));

    Runtime.getRuntime()
        .addShutdownHook(new Thread(() -> shutdown(serverContext, connectionHandler)));

    ByteBufferPool.init();

    Server server = new Server(serverContext, connectionHandler);
    server.start();

    initServerNode(serverContext);
    startRuntimeThreads(serverContext);
    Log.init(serverContext);
    startPermanentJobs(serverContext);

    System.out.println("NODE_READY port=" + port + " kad=" + serverContext.getOwnNodeId());

    waitForStopSignal();
    shutdown(serverContext, connectionHandler);
    System.exit(0);
  }

  private static void configureSettings() {
    Settings.knownNodes = parseKnownNodes(System.getProperty("redpanda.knownNodes", ""));
    Settings.MIN_CONNECTIONS = Integer.getInteger("redpanda.minConnections", 0);
    Settings.MAX_CONNECTIONS = Integer.getInteger("redpanda.maxConnections", 5);
    Settings.STD_PORT = Integer.getInteger("redpanda.stdPort", Settings.STD_PORT);
    Server.setShuttingDown(false);
  }

  private static String[] parseKnownNodes(String raw) {
    if (raw.trim().isEmpty()) {
      return new String[0];
    }
    return Arrays.stream(raw.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toArray(String[]::new);
  }

  private static void initServerNode(ServerContext serverContext) {
    Node serverNode = serverContext.getNodeStore().get(serverContext.getNodeId().getKademliaId());
    if (serverNode == null) {
      serverNode = new Node(serverContext, serverContext.getNodeId());
    }
    serverContext.setNode(serverNode);
    serverContext.getNodeStore().getNodeGraph().addVertex(serverNode);
  }

  private static void startRuntimeThreads(ServerContext serverContext) {
    Settings.init(serverContext);

    if (Server.getOutboundHandler() == null) {
      Server.setOutboundHandler(new OutboundHandler(serverContext));
    }
    Server.getOutboundHandler().start();
    new PeerPerformanceTestSchedulerJob(serverContext).start();
    new RequestPeerListJob(serverContext).start();
    new NodeStoreMaintainJob(serverContext).start();
  }

  private static void startPermanentJobs(ServerContext serverContext) {
    new PeerJobs(serverContext).start();
    new SaveJobs(serverContext).start();
    new GMManagerCleanJobs(serverContext).start();
    new KadRefreshJob(serverContext).start();
    new NodeInfoSetRefreshJob(serverContext).start();
    new NodeConnectionPointsSeenJob(serverContext).start();
    new UpTimeReporterJob(serverContext).start();
    new ServerRestartJob(serverContext).start();
    new OutboundCleanupJob(serverContext).start();
  }

  private static void waitForStopSignal() throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    String line;
    while ((line = reader.readLine()) != null) {
      if ("stop".equalsIgnoreCase(line.trim())) {
        break;
      }
    }
  }

  private static void shutdown(ServerContext serverContext, ConnectionHandler connectionHandler) {
    if (!shuttingDown.compareAndSet(false, true)) {
      return;
    }
    try {
      Server.setShuttingDown(true);
      ConnectionHandler.selector.wakeup();
      Server.shutdown(serverContext);
      NodeStore.threadPool.shutdownNow();
      Server.threadPool.shutdownNow();
      shutdownJobScheduler();
      shutdownConnectionReaderPool();
      joinQuietly(connectionHandler, TimeUnit.SECONDS.toMillis(5));
      joinQuietly(Server.getOutboundHandler(), TimeUnit.SECONDS.toMillis(5));
    } catch (Throwable t) {
      t.printStackTrace();
    }
    System.out.println(
        "NODE_STOPPED kad=" + Objects.requireNonNullElse(serverContext.getOwnNodeId(), "unknown"));
  }

  private static void shutdownJobScheduler() {
    try {
      java.lang.reflect.Field field = JobScheduler.class.getDeclaredField("jobScheduler");
      field.setAccessible(true);
      JobScheduler scheduler = (JobScheduler) field.get(null);
      scheduler.shutdownNow();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void shutdownConnectionReaderPool() {
    try {
      im.redpanda.transport.ConnectionReaderThread.threadPool.shutdownNow();
    } catch (Throwable ignored) {
      // best effort shutdown only used in tests
    }
  }

  private static void joinQuietly(Thread thread, long timeoutMillis) {
    if (thread == null) {
      return;
    }
    try {
      thread.join(timeoutMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
