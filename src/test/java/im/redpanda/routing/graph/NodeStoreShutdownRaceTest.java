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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
   * for it; neither side may end up in the recovery path. The test above is the deterministic pin;
   * this one is a regression guard for the concurrent case — with the lifecycle lock it cannot
   * fail, without the lock it fails only when the timing happens to line up (it did not in one
   * negative-control run, so do not read a green run here as proof on its own).
   */
  @Test
  void close_whileASaveIsInFlight_neverTriggersTheRecovery() throws Exception {
    ServerContext serverContext = contextWithStore();
    NodeStore original = nodeStore;
    for (int i = 0; i < 200; i++) {
      new Node(serverContext, new NodeId());
    }

    AtomicReference<Throwable> saverFailure = new AtomicReference<>();
    AtomicInteger saves = new AtomicInteger();
    CountDownLatch firstSaveDone = new CountDownLatch(1);
    Thread saver =
        new Thread(
            () -> {
              try {
                while (!original.isClosed()) {
                  original.saveToDisk();
                  saves.incrementAndGet();
                  firstSaveDone.countDown();
                }
              } catch (Throwable t) {
                saverFailure.set(t);
              } finally {
                firstSaveDone.countDown();
              }
            },
            "SaveJobs-under-test");
    saver.start();
    // At least one save has run against the live store before the close (Copilot review: a
    // "thread started" latch would let a slow runner close before the first save was attempted).
    assertThat(firstSaveDone.await(30, TimeUnit.SECONDS)).isTrue();
    // Then let the saver get into the middle of a later flush before pulling the store away.
    Thread.sleep(50);

    original.close();
    saver.join(TimeUnit.SECONDS.toMillis(30));

    assertThat(saver.isAlive()).as("the saver must observe the close and stop").isFalse();
    assertThat(saverFailure.get()).as("no exception escaped saveToDisk").isNull();
    assertThat(saves.get()).as("the saver did run against the live store").isPositive();
    assertThat(serverContext.getNodeStore())
        .as("closing under a save must not be mistaken for a corrupt cache")
        .isSameAs(original);
    assertThat(cachePath).exists();
  }
}
