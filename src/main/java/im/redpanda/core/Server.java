package im.redpanda.core;

import im.redpanda.identity.KademliaId;
import im.redpanda.ops.Settings;
import im.redpanda.routing.PeerPerformanceTestSchedulerJob;
import im.redpanda.routing.graph.NodeStoreMaintainJob;
import im.redpanda.transport.ConnectionHandler;
import im.redpanda.transport.ConnectionReaderThread;
import im.redpanda.transport.OutboundHandler;
import im.redpanda.transport.Peer;
import im.redpanda.transport.RequestPeerListJob;
import im.redpanda.transport.Saver;
import im.redpanda.updater.HTTPServer;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {

  /**
   * Protocol version. 23 = MS03 crypto (Ed25519/X25519/AES-256-GCM). The retired v22 protocol
   * (brainpool/AES-CTR) was shut down in the sdd02 phase-1 release (2026-07, MS03 Decision 10) and
   * its code path removed in phase 2; see {@link ConnectionReaderThread#parseHandshake}.
   */
  public static final int VERSION = 23;

  public static final String MAGIC = "k3gV";
  private static volatile boolean shuttingDown = false;

  /**
   * Serializes {@link #shutdown(ServerContext)} and carries the "already done" state.
   *
   * <p>Separate from {@link #shuttingDown}, which is the "stop doing work" signal every other
   * thread polls and which callers may set themselves <i>before</i> calling shutdown ({@code
   * TestNodeLauncher}) — folding the two together would turn such a shutdown into a no-op.
   */
  private static final Object SHUTDOWN_LOCK = new Object();

  /**
   * Guarded by {@link #SHUTDOWN_LOCK}. Set only after the shutdown work completed without error.
   */
  private static boolean shutdownCompleted = false;

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

  /**
   * Persists the node's state and closes the {@code NodeStore}. Idempotent (TD223): the work runs
   * once, a call arriving after it completed returns immediately, and a call arriving while it is
   * in flight waits for it.
   *
   * <p>A job-triggered restart calls this twice: {@code ServerRestartJob.work()} calls it and its
   * following {@code System.exit(0)} runs the JVM shutdown hook of {@code App}, which calls it
   * again — a second {@code savePeers} plus {@code localSettings.save} against an already closed
   * store, on a node that is on its way out. {@code ListenConsole}'s {@code e} command does the
   * same. The guard sits here rather than in the callers because every call site pairs up with a
   * hook that always runs on {@code System.exit}, and none of them may simply drop its call: the
   * E2E harness {@code TestNodeLauncher} goes through a wrapper of its own that both its JVM hook
   * and its {@code startNode} path invoke (guarded there by a separate {@code AtomicBoolean}), so
   * removing the direct call from a caller here would only move the same problem one level up.
   *
   * <p>Two details the guard has to get right (both from the adversarial review of this PR):
   *
   * <ul>
   *   <li>A shutdown that <b>threw</b> does not count as done — {@code shutdownCompleted} is set
   *       after the last step, so a caller arriving later still attempts the save and the close. A
   *       plain "claimed" flag would have turned e.g. a failing {@code savePeers} (full disk) into
   *       a node that never closes MapDB and never saves its settings.
   *   <li>A caller arriving <b>while</b> a shutdown is in flight waits for it instead of returning
   *       at once — that is what the monitor is for. Otherwise a SIGTERM during {@code
   *       ServerRestartJob}'s shutdown would let the JVM halt as soon as the hook returns, killing
   *       the job thread in the middle of {@code savePeers} or {@code NodeStore.close()}.
   * </ul>
   *
   * <p>{@link #setShuttingDown(boolean)} resets the guard, so a test harness that starts and stops
   * several nodes in one JVM keeps working ({@code TestNodeLauncher.configureSettings()}).
   */
  public static void shutdown(ServerContext serverContext) {
    synchronized (SHUTDOWN_LOCK) {
      if (shutdownCompleted) {
        log.info("shutdown already completed, skipping this call");
        return;
      }

      Server.shuttingDown = true;

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        log.warn("Interrupted during shutdown", e);
        Thread.currentThread().interrupt();
      }

      Saver.savePeers(serverContext.getPeerList());
      serverContext.getNodeStore().close();
      serverContext.getLocalSettings().save(serverContext.getPort());

      // Last statement on purpose: a shutdown that threw did NOT persist everything, and the
      // caller that follows (the JVM hook after ServerRestartJob's System.exit) is the retry.
      shutdownCompleted = true;
    }
  }

  public void start() {
    connectionHandler.start();
  }

  public static boolean isShuttingDown() {
    return shuttingDown;
  }

  /**
   * Sets the "stop doing work" signal. Clearing it also re-arms {@link #shutdown(ServerContext)},
   * which is how a single JVM can run several node lifecycles in sequence (tests).
   */
  public static void setShuttingDown(boolean shuttingDown) {
    Server.shuttingDown = shuttingDown;
    if (!shuttingDown) {
      synchronized (SHUTDOWN_LOCK) {
        shutdownCompleted = false;
      }
    }
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
