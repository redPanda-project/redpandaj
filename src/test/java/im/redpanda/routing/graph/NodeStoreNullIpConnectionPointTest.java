package im.redpanda.routing.graph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.gson.JsonObject;
import im.redpanda.core.LocalSettings;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.ArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * T150: a connection point without an ip must never reach the node cache, and one that somehow did
 * must not take the whole store with it.
 *
 * <p>The incident these tests pin down (testnet deploy #9, 2026-09-04): {@code
 * NodeConnectionPointsSeenJob} calls {@code node.seen(peer.getIp(), peer.getPort())} every two
 * minutes for every connected peer, and since T120a/#354 a connected peer can have {@code getIp()
 * == null} — {@code PeerList.addLocked} takes the address away from a peer whose address is claimed
 * by another identity. {@code Node} accepted the null, {@code NodeCodec} wrote it as a JSON null
 * and refused to read one back. Fifteen minutes later {@code NodeStoreMaintainJob} ran {@code
 * saveToDisk()}, MapDB deserialized the tier inside {@code clearWithExpire()} and threw, the
 * recovery installed a store with null tiers (TD185), and from then on every {@code
 * Node.getByKademliaId()} threw an NPE — which {@code ConnectionHandler.setupConnection} turns into
 * a dropped connection, so the node stopped accepting anything at all.
 *
 * <p>The port is unique to this class: the cache file name is derived from it and the surefire
 * forks share a working directory (see the T70 fork-CWD collision).
 */
class NodeStoreNullIpConnectionPointTest {

  private static final int PORT = 59712;

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

  /** The writer side: a peer without an address produces no connection point. */
  @Test
  void seen_withoutIp_addsNoConnectionPoint() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    Node node = new Node(serverContext, new NodeId());

    node.seen(null, 0);

    assertThat(node.getConnectionPoints()).isEmpty();
    // Being talked to is still worth recording, it is only the address that is unusable.
    assertThat(node.getLastSeen()).isGreaterThan(0L);
  }

  @Test
  void addConnectionPoint_withoutIp_isRejected() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    Node node = new Node(serverContext, new NodeId());

    assertThat(node.addConnectionPoint(null, 59558)).isFalse();
    assertThat(node.getConnectionPoints()).isEmpty();
  }

  /**
   * The codec side, independent of who produced the point: a node holding an ip-less connection
   * point (restored from a file written by an older build, say) must still round-trip. Writer and
   * reader disagreed — Gson wrote a JSON null, {@code nodeFromJson} threw {@code IOException:
   * connection point without an ip} on it.
   */
  @Test
  void codec_roundTripsANodeWhoseConnectionPointHasNoIp() {
    ArrayList<Node.ConnectionPoint> points = new ArrayList<>();
    points.add(new Node.ConnectionPoint(null, 0, System.currentTimeMillis(), 0));
    points.add(new Node.ConnectionPoint("10.0.0.5", 59558, System.currentTimeMillis(), 3));
    Node node = new Node(new NodeId(), System.currentTimeMillis(), points, 1, 2, 0L);

    JsonObject json = NodeCodec.nodeToJson(node);

    assertThatCode(
            () -> {
              Node restored = NodeCodec.nodeFromJson(json);
              assertThat(restored.getNodeId().getKademliaId())
                  .isEqualTo(node.getNodeId().getKademliaId());
              // The unusable hint is dropped, the usable one survives.
              assertThat(restored.getConnectionPoints()).hasSize(1);
              assertThat(restored.getConnectionPoints().getFirst().getIp()).isEqualTo("10.0.0.5");
              assertThat(restored.getConnectionPoints().getFirst().getRetries()).isEqualTo(3);
            })
        .doesNotThrowAnyException();
  }

  /**
   * The whole chain, as it ran on the testnet: a connected peer without an address is reported to
   * its node, the maintenance job saves — and the node cache is still readable afterwards.
   *
   * <p>On 8dcbb74 this fails at the very first {@code get()} after the save with {@code
   * NullPointerException: Cannot invoke "org.mapdb.HTreeMap.get(Object)" because "this.onHeap" is
   * null}.
   */
  @Test
  void saveToDisk_afterAPeerWithoutAnAddressWasSeen_leavesAUsableStore() {
    ServerContext serverContext = new ServerContext();
    serverContext.setPort(PORT);
    serverContext.setLocalSettings(new LocalSettings());
    nodeStore = NodeStore.buildWithDiskCache(serverContext);
    serverContext.setNodeStore(nodeStore);

    NodeId nodeId = new NodeId();
    Node node = new Node(serverContext, nodeId);
    node.seen(null, 0);

    serverContext.getNodeStore().saveToDisk();

    // The store the node keeps working with must be readable. Not "the same store": a genuinely
    // corrupt cache may legitimately be replaced -- but never by one that cannot be read.
    nodeStore = serverContext.getNodeStore();
    assertThatCode(() -> serverContext.getNodeStore().get(nodeId.getKademliaId()))
        .doesNotThrowAnyException();
    assertThat(serverContext.getNodeStore().get(nodeId.getKademliaId())).isNotNull();
    // ... and the routing graph it hands to the DHT jobs is the persisted one, not a fresh empty
    // graph whose vertices nobody knows ("no such vertex in graph", 110 times in 40 minutes).
    assertThat(serverContext.getNodeStore().getNodeGraph())
        .isSameAs(serverContext.getLocalSettings().getNodeGraph());
  }

  /**
   * The recovery machinery itself (TD185), driven into its catch block rather than around it: the
   * test above only proves that the flush no longer fails. Here the flush is made to fail — the
   * tiers are closed under it, which is what MapDB throwing out of {@code clearWithExpire()} looks
   * like from {@code saveToDisk()}'s point of view — and the store that comes out has to be one the
   * node can keep working with.
   *
   * <p>On 8dcbb74 the replacement was {@code new NodeStore(serverContext)}: null tiers, empty
   * graph, an NPE on every read.
   */
  @Test
  void saveToDisk_whenTheFlushFails_installsAUsableStoreOnTheSameGraphAndLock() {
    ServerContext serverContext = new ServerContext();
    serverContext.setPort(PORT);
    serverContext.setLocalSettings(new LocalSettings());
    NodeStore broken = NodeStore.buildWithDiskCache(serverContext);
    serverContext.setNodeStore(broken);

    NodeId nodeId = new NodeId();
    new Node(serverContext, nodeId);
    Object graphBefore = broken.getNodeGraph();
    Object lockBefore = broken.getReadWriteLock();

    broken.close();
    broken.saveToDisk();

    nodeStore = serverContext.getNodeStore();
    assertThat(nodeStore).as("the broken store must have been replaced").isNotSameAs(broken);

    // Usable: a read does not throw and a write survives a read-back. This is the whole point --
    // Node.getByKademliaId() is on the inbound-connection path, so a store that throws costs the
    // node every new connection.
    NodeId fresh = new NodeId();
    assertThatCode(
            () -> {
              nodeStore.get(nodeId.getKademliaId());
              nodeStore.put(fresh.getKademliaId(), new Node(serverContext, fresh));
            })
        .doesNotThrowAnyException();
    assertThat(nodeStore.get(fresh.getKademliaId())).isNotNull();

    // Same graph object AND same lock object, so LocalSettings' registered read lock still guards
    // the graph the successor mutates.
    assertThat(nodeStore.getNodeGraph()).isSameAs(graphBefore);
    assertThat(nodeStore.getReadWriteLock()).isSameAs(lockBefore);
    assertThat(serverContext.getLocalSettings().getNodeGraph()).isSameAs(graphBefore);
  }

  /**
   * TD184: MapDB's {@code HTreeMap.close()} shuts down the executor it was handed. While that was
   * the JVM-wide static {@code NodeStore.threadPool}, closing one store killed the expiry threads
   * of every store built afterwards — which is exactly what the recovery path above does, so the
   * rebuilt store would have died on {@code createOrOpen} with a {@code
   * RejectedExecutionException}.
   */
  @Test
  void aStoreCanBeBuiltAfterAnotherOneWasClosed() {
    ServerContext serverContext = new ServerContext();
    serverContext.setPort(PORT);
    serverContext.setLocalSettings(new LocalSettings());

    NodeStore first = NodeStore.buildWithDiskCache(serverContext);
    first.close();

    assertThatCode(
            () -> {
              nodeStore = NodeStore.buildWithDiskCache(serverContext);
              serverContext.setNodeStore(nodeStore);
              nodeStore.get(new NodeId().getKademliaId());
            })
        .doesNotThrowAnyException();
  }
}
