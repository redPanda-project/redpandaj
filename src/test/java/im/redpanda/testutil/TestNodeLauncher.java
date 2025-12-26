package im.redpanda.testutil;

import im.redpanda.core.ByteBufferPool;
import im.redpanda.core.ConnectionHandler;
import im.redpanda.core.LocalSettings;
import im.redpanda.core.Log;
import im.redpanda.core.Node;
import im.redpanda.core.OutboundHandler;
import im.redpanda.core.PeerJobs;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import im.redpanda.core.Settings;
import im.redpanda.jobs.GMManagerCleanJobs;
import im.redpanda.jobs.JobScheduler;
import im.redpanda.jobs.KadRefreshJob;
import im.redpanda.jobs.NodeConnectionPointsSeenJob;
import im.redpanda.jobs.NodeInfoSetRefreshJob;
import im.redpanda.jobs.NodeStoreMaintainJob;
import im.redpanda.jobs.PeerPerformanceTestSchedulerJob;
import im.redpanda.jobs.RequestPeerListJob;
import im.redpanda.jobs.SaveJobs;
import im.redpanda.jobs.ServerRestartJob;
import im.redpanda.jobs.UpTimeReporterJob;
import im.redpanda.store.NodeStore;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

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

/**
 * Lightweight launcher used only from E2E tests. It mirrors the application boot sequence without the interactive
 * console and accepts a simple "stop" command on stdin to trigger a graceful shutdown.
 */
public final class TestNodeLauncher {

    private static final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private TestNodeLauncher() {
    }

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
        serverContext.setNonce(serverContext.getLocalSettings().getMyIdentity().getKademliaId());
        serverContext.setNodeStore(NodeStore.buildWithDiskCache(serverContext));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown(serverContext, connectionHandler)));

        ByteBufferPool.init();

        Server server = new Server(serverContext, connectionHandler);
        server.start();

        initServerNode(serverContext);
        startRuntimeThreads(serverContext);
        Log.init(serverContext);
        startPermanentJobs(serverContext);

        System.out.println("NODE_READY port=" + port + " kad=" + serverContext.getNonce());

        waitForStopSignal();
        shutdown(serverContext, connectionHandler);
        System.exit(0);
    }

    private static void configureSettings() {
        Settings.knownNodes = parseKnownNodes(System.getProperty("redpanda.knownNodes", ""));
        Settings.MIN_CONNECTIONS = Integer.getInteger("redpanda.minConnections", 0);
        Settings.MAX_CONNECTIONS = Integer.getInteger("redpanda.maxConnections", 5);
        Settings.STD_PORT = Integer.getInteger("redpanda.stdPort", Settings.STD_PORT);
        Server.shuttingDown = false;
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

        if (Server.outboundHandler == null) {
            Server.outboundHandler = new OutboundHandler(serverContext);
        }
        Server.outboundHandler.start();
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
            Server.shuttingDown = true;
            ConnectionHandler.selector.wakeup();
            Server.shutdown(serverContext);
            NodeStore.threadPool.shutdownNow();
            Server.threadPool.shutdownNow();
            shutdownJobScheduler();
            shutdownConnectionReaderPool();
            joinQuietly(connectionHandler, TimeUnit.SECONDS.toMillis(5));
            joinQuietly(Server.outboundHandler, TimeUnit.SECONDS.toMillis(5));
        } catch (Throwable t) {
            t.printStackTrace();
        }
        System.out.println("NODE_STOPPED kad=" + Objects.requireNonNullElse(serverContext.getNonce(), "unknown"));
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
            im.redpanda.core.ConnectionReaderThread.threadPool.shutdownNow();
        } catch (Throwable ignored) {
            //best effort shutdown only used in tests
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
