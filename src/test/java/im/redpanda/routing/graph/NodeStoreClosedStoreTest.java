package im.redpanda.routing.graph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import im.redpanda.core.LocalSettings;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What happens to a {@link NodeStore} reference once the store behind it is <em>closed</em>.
 *
 * <p>Two ways a store gets closed under a live reference: {@link NodeStore#saveToDisk()}'s recovery
 * path closes the store it replaces (TD185/T150), and {@code Server.shutdown()} closes the live one
 * without a replacement. Both used to be unhandled:
 *
 * <ul>
 *   <li>TD215 — the swap is not atomic with respect to threads that already hold a reference.
 *       {@code PeerPerformanceTestGarlicMessageJob} deliberately resolves the store once (it has to
 *       lock and unlock the same lock object), so it keeps working on the closed predecessor, whose
 *       MapDB tiers answer with {@code IllegalAccessError: Store was closed}. {@code put()}
 *       propagated that into {@code new Node(...)} — the inbound-connection path — and {@code
 *       get()} did not catch it either ({@code IllegalAccessError} is an {@code Error}, not an
 *       {@code Exception}).
 *   <li>TD212 — {@link NodeStore#size()} calls {@code saveToDisk()} and then read {@code
 *       this.onDisk}, i.e. the tier of the store that call may just have replaced.
 * </ul>
 *
 * <p>The port is unique to this class: the cache file name is derived from it and the surefire
 * forks share a working directory (see the T70 fork-CWD collision).
 */
class NodeStoreClosedStoreTest {

  private static final int PORT = 59714;

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  private final Path cachePath = Path.of(NodeStore.nodeCachePath(PORT));
  private ServerContext serverContext;
  private NodeStore firstStore;

  @BeforeEach
  void setUp() throws IOException {
    Files.createDirectories(cachePath.getParent());
    deleteCachePath();

    serverContext = new ServerContext();
    serverContext.setPort(PORT);
    serverContext.setLocalSettings(new LocalSettings());
  }

  @AfterEach
  void tearDown() throws IOException {
    if (firstStore != null) {
      firstStore.close();
    }
    if (serverContext.getNodeStore() != null) {
      serverContext.getNodeStore().close();
    }
    deleteCachePath();
  }

  /**
   * The cache path is a file normally, but {@link #saveToDisk_whenTheRebuildAlsoFails} makes it a
   * non-empty directory on purpose.
   */
  private void deleteCachePath() throws IOException {
    if (!Files.exists(cachePath)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(cachePath)) {
      for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(path);
      }
    }
  }

  /** A store whose flush is guaranteed to fail, plus the successor its recovery installed. */
  private NodeStore recoverAndReturnTheSuccessor() {
    firstStore = NodeStore.buildWithDiskCache(serverContext);
    serverContext.setNodeStore(firstStore);

    // One entry, otherwise there is nothing for the flush to push down into the (killed) on-disk
    // tier and clearWithExpire() never touches it -- no throw, no recovery.
    NodeId toFlush = new NodeId();
    firstStore.put(toFlush.getKademliaId(), unregisteredNode(toFlush));

    firstStore.breakDiskTierForTest();
    firstStore.saveToDisk();

    NodeStore successor = serverContext.getNodeStore();
    assertThat(successor)
        .as("precondition: the recovery must have replaced the store")
        .isNotSameAs(firstStore);
    assertThat(firstStore.isClosed()).as("precondition: the predecessor is closed").isTrue();
    return successor;
  }

  /** A node that is not registered in any store yet (the persistence-layer constructor). */
  private Node unregisteredNode(NodeId nodeId) {
    return new Node(nodeId, System.currentTimeMillis(), new ArrayList<>(), 0, 0, 0);
  }

  /**
   * TD215: a write through a reference to the closed predecessor has to land in the store the
   * recovery installed, not throw and not disappear.
   */
  @Test
  void put_throughAClosedPredecessor_landsInTheStoreTheRecoveryInstalled() {
    NodeStore successor = recoverAndReturnTheSuccessor();

    NodeId nodeId = new NodeId();
    KademliaId id = nodeId.getKademliaId();
    Node node = unregisteredNode(nodeId);
    assertThat(successor.get(id)).as("precondition: unknown to the successor").isNull();

    assertThatCode(() -> firstStore.put(id, node)).doesNotThrowAnyException();

    assertThat(successor.get(id))
        .as("the write must have landed in the live store, not in the closed tiers")
        .isSameAs(node);
    assertThat(firstStore.get(id))
        .as("and reading back through the same stale reference must find it")
        .isSameAs(node);
  }

  /**
   * TD215, read side: {@code get()}'s catch block only covers {@code Exception}, so MapDB's {@code
   * IllegalAccessError} went straight through it — and had it been caught, the diagnosis would have
   * been wrong ("corrupt cache" plus an {@code onDisk.clear()} on a closed tier).
   */
  @Test
  void getAndRemove_throughAClosedPredecessor_useTheStoreTheRecoveryInstalled() {
    NodeStore successor = recoverAndReturnTheSuccessor();

    NodeId nodeId = new NodeId();
    KademliaId id = nodeId.getKademliaId();
    Node node = unregisteredNode(nodeId);
    successor.put(id, node);

    assertThat(firstStore.get(id)).isSameAs(node);

    assertThatCode(() -> firstStore.remove(id)).doesNotThrowAnyException();
    assertThat(successor.get(id)).as("the removal must have reached the live store").isNull();
  }

  /**
   * TD212: {@code size()} is the one method that drives the recovery and then reads a tier, so it
   * closed the store under its own feet and read from it afterwards.
   */
  @Test
  void size_whenTheFlushItTriggersRecovers_readsTheSuccessorsTier() {
    firstStore = NodeStore.buildWithDiskCache(serverContext);
    serverContext.setNodeStore(firstStore);

    NodeId nodeId = new NodeId();
    firstStore.put(nodeId.getKademliaId(), unregisteredNode(nodeId));

    firstStore.breakDiskTierForTest();

    assertThatCode(firstStore::size).doesNotThrowAnyException();

    assertThat(serverContext.getNodeStore())
        .as("size() drives the very recovery that replaces the store")
        .isNotSameAs(firstStore);
    // The rebuilt on-disk tier starts empty: the entry was in the on-heap tier of a store whose
    // flush had already failed. Reporting 0 is the honest answer, throwing was not.
    assertThat(firstStore.size()).isZero();
    assertThat(serverContext.getNodeStore().size()).isZero();
  }

  /**
   * The other way a store ends up closed: {@code Server.shutdown()} closes the live store and
   * installs nothing. Late writes from job threads are then dropped instead of throwing.
   */
  @Test
  void accessAfterShutdownClosedTheLiveStore_isDroppedInsteadOfThrowing() {
    firstStore = NodeStore.buildWithDiskCache(serverContext);
    serverContext.setNodeStore(firstStore);

    firstStore.close();

    NodeId nodeId = new NodeId();
    KademliaId id = nodeId.getKademliaId();
    Node node = unregisteredNode(nodeId);

    assertThatCode(
            () -> {
              firstStore.put(id, node);
              firstStore.remove(id);
            })
        .doesNotThrowAnyException();
    assertThat(firstStore.get(id)).isNull();
    assertThat(firstStore.size()).isZero();
  }

  /**
   * TD185's second branch: the recovery's file-backed rebuild can fail on its own, and then it
   * falls back to {@link NodeStore#buildWithMemoryCacheOnly(ServerContext)}. Nothing covered that
   * branch so far. Made deterministic by turning the cache path into a non-empty
   * <em>directory</em>: the recovery's {@code Files.delete} fails with {@code
   * DirectoryNotEmptyException} (logged) and {@code DBMaker.fileDB} cannot open a directory either.
   */
  @Test
  void saveToDisk_whenTheRebuildAlsoFails_fallsBackToAUsableMemoryOnlyStore() throws IOException {
    firstStore = NodeStore.buildWithDiskCache(serverContext);
    serverContext.setNodeStore(firstStore);

    NodeId known = new NodeId();
    new Node(serverContext, known);
    Object graphBefore = firstStore.getNodeGraph();
    Object lockBefore = firstStore.getReadWriteLock();

    firstStore.breakDiskTierForTest();
    // The disk tier is closed, so the file handle is gone and the path can be replaced.
    deleteCachePath();
    Files.createDirectory(cachePath);
    Files.createFile(cachePath.resolve("not-a-cache"));

    firstStore.saveToDisk();

    NodeStore fallback = serverContext.getNodeStore();
    assertThat(fallback).as("the broken store must have been replaced").isNotSameAs(firstStore);
    assertThat(firstStore.isClosed()).isTrue();

    // Usable, which is the whole point: Node.getByKademliaId() is on the inbound-connection path.
    NodeId fresh = new NodeId();
    assertThatCode(
            () -> {
              fallback.get(known.getKademliaId());
              fallback.put(fresh.getKademliaId(), unregisteredNode(fresh));
            })
        .doesNotThrowAnyException();
    assertThat(fallback.get(fresh.getKademliaId())).isNotNull();
    // The cache CONTENTS do not survive a recovery -- the on-disk file was deleted as corrupt and
    // the tiers are new -- so `known` is legitimately gone from the cache and only the routing
    // graph is carried over (see below). What must hold is the exact count: a memory-only store has
    // no on-disk tier, so size() reports the on-heap one, and that holds the one node written after
    // the swap. "Non-negative" would have passed for a store that silently lost everything.
    assertThat(fallback.size()).as("a memory-only store sizes its on-heap tier").isEqualTo(1);
    assertThat(cachePath)
        .as("still the directory, so no file-backed tier was opened -- this is the fallback")
        .isDirectory();

    // Same graph and lock object, so LocalSettings' registered read lock still guards the graph
    // the successor mutates (see NodeStore#takeOverGraphGuardFrom).
    assertThat(fallback.getNodeGraph()).isSameAs(graphBefore);
    assertThat(fallback.getReadWriteLock()).isSameAs(lockBefore);
  }

  /**
   * The regression test for the T149 review's HIGH finding. {@code saveToDisk()} holds {@code
   * lifecycleLock} for the whole flush — and on the recovery path across {@code close()}, the file
   * delete and a full three-tier rebuild. The first version of {@code liveStore()} read the {@code
   * closed} flag under that lock, which put the 15-minute flush straight onto the
   * inbound-connection path: {@code ConnectionHandler.setupConnection} → {@code
   * Node.getByKademliaId} → {@code get()} would have waited for it, i.e. no new handshake completes
   * while a save runs.
   *
   * <p>Deterministic via the existing {@code betweenFlushStepsForTest} seam: the save is parked
   * inside the lock on a latch, so the reader either returns immediately or it is blocked — no
   * timing luck involved. The only wall-clock waits are the bounded joins of a negative assertion.
   */
  @Test
  void get_whileASaveHoldsTheLifecycleLock_doesNotBlock() throws Exception {
    firstStore = NodeStore.buildWithDiskCache(serverContext);
    serverContext.setNodeStore(firstStore);

    NodeId nodeId = new NodeId();
    firstStore.put(nodeId.getKademliaId(), unregisteredNode(nodeId));

    CountDownLatch saveIsInsideTheLock = new CountDownLatch(1);
    CountDownLatch readerIsDone = new CountDownLatch(1);
    NodeStore.betweenFlushStepsForTest =
        () -> {
          saveIsInsideTheLock.countDown();
          try {
            readerIsDone.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };

    Thread saver = new Thread(firstStore::saveToDisk, "t149-saver");
    try {
      saver.start();
      assertThat(saveIsInsideTheLock.await(10, TimeUnit.SECONDS))
          .as("the save must have reached the seam, i.e. it holds lifecycleLock")
          .isTrue();

      AtomicReference<Object> read = new AtomicReference<>();
      AtomicReference<Throwable> failed = new AtomicReference<>();
      Thread reader =
          new Thread(
              () -> {
                try {
                  read.set(firstStore.get(nodeId.getKademliaId()));
                } catch (Throwable t) {
                  failed.set(t);
                }
              },
              "t149-reader");
      reader.start();
      reader.join(2000);

      assertThat(reader.isAlive())
          .as("get() must not wait for an in-flight saveToDisk() -- it is on the handshake path")
          .isFalse();
      assertThat(failed.get()).isNull();
      // The flush pushed the entry down a tier, so the instance differs -- but it must still be
      // found through the overflow loader.
      assertThat(read.get()).isNotNull();
    } finally {
      readerIsDone.countDown();
      NodeStore.betweenFlushStepsForTest = null;
      saver.join(10_000);
    }
  }

  /**
   * The TD215 scenario as it actually happens: a thread hammering a cached reference while the
   * recovery swaps the store underneath it. Covers both the resolution through {@link
   * NodeStore#liveStore()} and its residual check-then-act window (which the accessors answer with
   * one retry) — that window cannot be hit deterministically without another production seam, so
   * this is a bounded stress run whose only failure mode is an escaping throwable.
   */
  @Test
  void put_concurrentWithTheRecovery_neverLetsAThrowableEscape() throws Exception {
    firstStore = NodeStore.buildWithDiskCache(serverContext);
    serverContext.setNodeStore(firstStore);

    NodeId toFlush = new NodeId();
    firstStore.put(toFlush.getKademliaId(), unregisteredNode(toFlush));

    CountDownLatch writerIsWarm = new CountDownLatch(1);
    AtomicReference<Throwable> failed = new AtomicReference<>();
    Thread writer =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < 4000; i++) {
                  // A 20-character id, which is what KademliaId(String) requires, and no keypair
                  // generation.
                  NodeId id = new NodeId(new KademliaId(String.format("%020d", i)));
                  firstStore.put(id.getKademliaId(), unregisteredNode(id));
                  firstStore.get(id.getKademliaId());
                  if (i == 20) {
                    writerIsWarm.countDown();
                  }
                }
              } catch (Throwable t) {
                failed.set(t);
              } finally {
                writerIsWarm.countDown();
              }
            },
            "t149-writer");
    writer.start();
    assertThat(writerIsWarm.await(10, TimeUnit.SECONDS)).isTrue();

    firstStore.breakDiskTierForTest();
    firstStore.saveToDisk();

    writer.join(30_000);
    assertThat(writer.isAlive()).isFalse();
    assertThat(failed.get())
        .as("a cached NodeStore reference must survive the swap without throwing")
        .isNull();
    assertThat(serverContext.getNodeStore()).isNotSameAs(firstStore);
  }
}
