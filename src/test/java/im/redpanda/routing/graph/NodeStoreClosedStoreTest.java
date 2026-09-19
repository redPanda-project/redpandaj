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
    assertThat(fallback.size()).as("a memory-only store sizes its on-heap tier").isNotNegative();

    // Same graph and lock object, so LocalSettings' registered read lock still guards the graph
    // the successor mutates (see NodeStore#takeOverGraphGuardFrom).
    assertThat(fallback.getNodeGraph()).isSameAs(graphBefore);
    assertThat(fallback.getReadWriteLock()).isSameAs(lockBefore);
  }
}
