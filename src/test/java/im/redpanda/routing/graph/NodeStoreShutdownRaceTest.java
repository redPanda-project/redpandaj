package im.redpanda.routing.graph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import im.redpanda.core.LocalSettings;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * REDPANDAJ-2EZ (testnet node1, 2026-09-05 01:46 UTC): {@code ServerRestartJob} and {@code
 * SaveJobs} tick from the same start instant (hourly vs. every 15 minutes), so a job-triggered
 * restart always coincides with a save. {@code Server.shutdown()} closed the tiers while {@code
 * saveToDisk()} was inside {@code clearWithExpire()}; MapDB threw {@code IllegalAccessError: Store
 * was closed} and the recovery path took that for a corrupt cache: Sentry event, cache file
 * deleted, store rebuilt — all a moment before {@code System.exit}. The node then came back with an
 * empty node cache.
 *
 * <p>The port is unique to this class: the cache file name is derived from it and the surefire
 * forks share a working directory (see the T70 fork-CWD collision).
 */
class NodeStoreShutdownRaceTest {

  private static final int PORT = 59713;

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  private final Path cachePath = Path.of(NodeStore.nodeCachePath(PORT));
  private NodeStore nodeStore;

  @BeforeEach
  void cleanCache() throws IOException {
    Files.createDirectories(cachePath.getParent());
    Files.deleteIfExists(cachePath);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (nodeStore != null) {
      nodeStore.close();
      nodeStore = null;
    }
    Files.deleteIfExists(cachePath);
  }

  private ServerContext contextWithStore() {
    ServerContext serverContext = new ServerContext();
    serverContext.setPort(PORT);
    serverContext.setLocalSettings(new LocalSettings());
    nodeStore = NodeStore.buildWithDiskCache(serverContext);
    serverContext.setNodeStore(nodeStore);
    return serverContext;
  }

  /** The shutdown order as it happened: close first, then the save that was already scheduled. */
  @Test
  void saveToDisk_afterClose_isANoOpAndKeepsTheCacheFile() {
    ServerContext serverContext = contextWithStore();
    new Node(serverContext, new NodeId());
    NodeStore closedStore = nodeStore;

    closedStore.close();

    assertThatCode(closedStore::saveToDisk).doesNotThrowAnyException();
    assertThat(serverContext.getNodeStore())
        .as("a closed store is not a broken store: no recovery, no replacement")
        .isSameAs(closedStore);
    assertThat(cachePath).as("the recovery's Files.delete must not have run").exists();
  }

  @Test
  void close_isIdempotent() {
    contextWithStore();

    nodeStore.close();

    assertThatCode(nodeStore::close).doesNotThrowAnyException();
    assertThat(nodeStore.isClosed()).isTrue();
  }

  /**
   * The other interleaving: the save is in flight when shutdown closes the store. Close has to wait
   * for it, and neither side may end up in the recovery path. Deterministic: the test seam runs
   * between the flush steps while the save holds the lifecycle lock; from there the closer thread
   * is started and the seam returns only once that thread is parked on the lock ({@code
   * Thread.State.BLOCKED}). Without the lock the closer never blocks, closes the tiers under the
   * flush, and the third {@code clearWithExpire()} drives the recovery (store replaced, file gone).
   */
  @Test
  void close_whileASaveIsInFlight_waitsForTheSaveAndSkipsTheRecovery() throws Exception {
    ServerContext serverContext = contextWithStore();
    NodeStore original = nodeStore;
    for (int i = 0; i < 20; i++) {
      new Node(serverContext, new NodeId());
    }

    Thread closer = new Thread(original::close, "Server.shutdown-under-test");
    AtomicBoolean closerParkedOnTheLock = new AtomicBoolean();
    AtomicBoolean closedDuringTheFlush = new AtomicBoolean();
    NodeStore.betweenFlushStepsForTest =
        () -> {
          closer.start();
          long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
          while (closer.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
            Thread.onSpinWait();
          }
          closerParkedOnTheLock.set(closer.getState() == Thread.State.BLOCKED);
          closedDuringTheFlush.set(original.isClosed());
        };
    try {
      assertThatCode(original::saveToDisk).doesNotThrowAnyException();
    } finally {
      NodeStore.betweenFlushStepsForTest = null;
    }
    closer.join(TimeUnit.SECONDS.toMillis(30));

    assertThat(closerParkedOnTheLock).as("close() must wait for the in-flight save").isTrue();
    assertThat(closedDuringTheFlush).as("the tiers were still open while the save ran").isFalse();
    assertThat(closer.isAlive()).as("close() completes once the save is done").isFalse();
    assertThat(original.isClosed()).isTrue();
    assertThat(serverContext.getNodeStore())
        .as("closing under a save must not be mistaken for a corrupt cache")
        .isSameAs(original);
    assertThat(cachePath).exists();
  }
}
