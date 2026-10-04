package im.redpanda.routing.graph;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import im.redpanda.core.LocalSettings;
import im.redpanda.core.NodeIdCodec;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import im.redpanda.ops.Settings;
import im.redpanda.testutil.ConcurrencyTestSupport;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * T136/TD174: the node graph lives in {@code data/nodeGraph<port>.json} instead of inside {@code
 * LocalSettings}, and a node that starts on a pre-T136 settings file (graph embedded) migrates it.
 *
 * <p>The port is unique to this class: file names derive from it and the surefire forks share a
 * working directory (see the T70 fork-CWD collision).
 */
class NodeGraphFileTest {

  private static final int PORT = 59716;

  /** Settings file written by the pre-T136 code (2 nodes, 1 edge of weight 17, embedded). */
  private static final String PRE_T136_FIXTURE = "/fixtures/localSettings_pre_t136.json";

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  private NodeStore nodeStore;

  @BeforeEach
  void setUp() throws IOException {
    new File(Settings.SAVE_DIR).mkdir();
    deleteFiles();
  }

  @AfterEach
  void tearDown() throws IOException {
    if (nodeStore != null) {
      nodeStore.close();
      nodeStore = null;
    }
    deleteFiles();
  }

  private static void deleteFiles() throws IOException {
    Files.deleteIfExists(NodeGraphFile.file(PORT).toPath());
    Files.deleteIfExists(NodeGraphFile.tmpFile(PORT).toPath());
    Files.deleteIfExists(LocalSettings.settingsFile(PORT).toPath());
    Files.deleteIfExists(LocalSettings.tmpSettingsFile(PORT).toPath());
    Files.deleteIfExists(Path.of(NodeStore.nodeCachePath(PORT)));
  }

  /** The edge state has to come back as it was, not as "checked just now" (moved from T117). */
  @Test
  void roundtripKeepsNodeAndEdgeState() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    DefaultDirectedWeightedGraph<Node, NodeEdge> graph =
        new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    Node a = new Node(serverContext, new NodeId());
    a.seen("10.0.0.1", 59558);
    a.setGmTestsSuccessful(7);
    Node b = new Node(serverContext, new NodeId());
    graph.addVertex(a);
    graph.addVertex(b);
    NodeEdge edge = graph.addEdge(a, b);
    graph.setEdgeWeight(edge, 17d);
    edge.setLastCheckFailed(true);
    long timeLastCheckFailed = edge.getTimeLastCheckFailed();

    assertThat(NodeGraphFile.save(PORT, graph, null, null)).isTrue();
    DefaultDirectedWeightedGraph<Node, NodeEdge> loaded = NodeGraphFile.load(PORT, null);

    assertThat(loaded.vertexSet()).hasSize(2);
    Node loadedA = loaded.vertexSet().stream().filter(n -> n.equals(a)).findFirst().orElseThrow();
    assertThat(loadedA.getGmTestsSuccessful()).isEqualTo(7);
    assertThat(loadedA.latestSeenConnectionPoint().getIp()).isEqualTo("10.0.0.1");
    assertThat(loadedA.latestSeenConnectionPoint().getPort()).isEqualTo(59558);
    assertThat(loaded.edgeSet()).hasSize(1);
    NodeEdge loadedEdge = loaded.edgeSet().iterator().next();
    assertThat(loaded.getEdgeWeight(loadedEdge)).isEqualTo(17d);
    assertThat(loadedEdge.isLastCheckFailed()).isTrue();
    assertThat(loadedEdge.getTimeLastCheckFailed()).isEqualTo(timeLastCheckFailed);
    assertThat(NodeGraphFile.tmpFile(PORT)).doesNotExist();
  }

  /** A node on two edges must come back as ONE object, not one copy per edge (moved from T117). */
  @Test
  void roundtripKeepsSharedVertexInstances() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    DefaultDirectedWeightedGraph<Node, NodeEdge> graph =
        new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    Node hub = new Node(serverContext, new NodeId());
    Node left = new Node(serverContext, new NodeId());
    Node right = new Node(serverContext, new NodeId());
    for (Node node : new Node[] {hub, left, right}) {
      graph.addVertex(node);
    }
    graph.addEdge(left, hub);
    graph.addEdge(hub, right);

    NodeGraphFile.save(PORT, graph, null, null);
    DefaultDirectedWeightedGraph<Node, NodeEdge> loaded = NodeGraphFile.load(PORT, null);

    assertThat(loaded.vertexSet()).hasSize(3);
    NodeEdge toHub =
        loaded.edgeSet().stream()
            .filter(e -> loaded.getEdgeTarget(e).equals(hub))
            .findFirst()
            .orElseThrow();
    NodeEdge fromHub =
        loaded.edgeSet().stream()
            .filter(e -> loaded.getEdgeSource(e).equals(hub))
            .findFirst()
            .orElseThrow();
    assertThat(loaded.getEdgeTarget(toHub)).isSameAs(loaded.getEdgeSource(fromHub));
  }

  /**
   * The graph is the very object NodeStore mutates under its write lock (REDPANDAJ-2DW), so
   * saveGraph() must encode under the matching read lock. Asserted via the lock itself — a save
   * that takes the read lock cannot make progress while the write lock is held (moved from the
   * LocalSettings test, where this lived until T136).
   */
  @Test
  @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
  void saveGraphBlocksWhileTheWriteLockIsHeld() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    serverContext.setPort(PORT);
    NodeStore store = serverContext.getNodeStore();
    store.getNodeGraph().addVertex(new Node(serverContext, new NodeId()));

    ConcurrencyTestSupport.assertBlockedWhileHeld(
        store.getReadWriteLock().writeLock(), store::saveGraph);

    assertThat(NodeGraphFile.load(PORT, null).vertexSet()).hasSize(1);
  }

  /** A save that fails half way through the encoding must leave the previous file intact. */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  void failedSaveKeepsPreviousFile() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    DefaultDirectedWeightedGraph<Node, NodeEdge> graph =
        new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    graph.addVertex(new Node(serverContext, new NodeId()));
    assertThat(NodeGraphFile.save(PORT, graph, null, null)).isTrue();
    byte[] savedFile = Files.readAllBytes(NodeGraphFile.file(PORT).toPath());

    // a vertex that is not a Node makes the encoder throw in the middle of the graph
    ((DefaultDirectedWeightedGraph) graph).addVertex(new Object());

    assertThat(NodeGraphFile.save(PORT, graph, null, null)).isFalse();
    assertThat(Arrays.equals(savedFile, Files.readAllBytes(NodeGraphFile.file(PORT).toPath())))
        .as("the graph file must be byte identical to the last successful save")
        .isTrue();
    assertThat(NodeGraphFile.tmpFile(PORT)).doesNotExist();
  }

  /**
   * The deploy case: a node restarts on the new build with a settings file the old build wrote.
   * Identity and graph must both survive, the graph must end up in its own file and leave the
   * settings file, and the next start must read it from there.
   */
  @Test
  void preT136SettingsFile_migratesTheEmbeddedGraphIntoItsOwnFile() throws Exception {
    byte[] fixture = readFixture();
    Files.write(LocalSettings.settingsFile(PORT).toPath(), fixture);
    KademliaId expectedIdentity = fixtureIdentity(fixture);
    Set<KademliaId> expectedNodes = fixtureNodeIds(fixture);

    // first start on the new build
    LocalSettings settings = LocalSettings.load(PORT);
    assertThat(settings.getMyIdentity().getKademliaId()).isEqualTo(expectedIdentity);
    nodeStore = NodeStore.buildWithDiskCache(contextFor(settings));

    assertGraphIsTheFixtureGraph(nodeStore.getNodeGraph(), expectedNodes);
    assertThat(NodeGraphFile.file(PORT)).as("written right away, not at the next save").exists();
    assertThat(settings.pendingLegacyNodeGraph()).isNull();

    settings.save(PORT);
    JsonObject settingsJson =
        JsonParser.parseString(Files.readString(LocalSettings.settingsFile(PORT).toPath()))
            .getAsJsonObject();
    assertThat(settingsJson.getAsJsonObject("nodeGraph").getAsJsonArray("vertices"))
        .as("the settings file only keeps the empty placeholder")
        .isEmpty();
    nodeStore.close();
    nodeStore = null;

    // second start: the graph now comes from its own file
    LocalSettings restarted = LocalSettings.load(PORT);
    assertThat(restarted.getMyIdentity().getKademliaId()).isEqualTo(expectedIdentity);
    assertThat(restarted.pendingLegacyNodeGraph()).isNull();
    nodeStore = NodeStore.buildWithDiskCache(contextFor(restarted));

    assertGraphIsTheFixtureGraph(nodeStore.getNodeGraph(), expectedNodes);
  }

  /**
   * After a rollback to a pre-T136 build and forward again, the graph embedded in the settings is
   * the newer one: it wins over the (stale) graph file and replaces it.
   */
  @Test
  void embeddedGraphWinsOverAnExistingGraphFile() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    DefaultDirectedWeightedGraph<Node, NodeEdge> stale =
        new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    stale.addVertex(new Node(serverContext, new NodeId()));
    NodeGraphFile.save(PORT, stale, null, null);

    byte[] fixture = readFixture();
    Files.write(LocalSettings.settingsFile(PORT).toPath(), fixture);
    LocalSettings settings = LocalSettings.load(PORT);

    assertGraphIsTheFixtureGraph(NodeGraphFile.load(PORT, settings), fixtureNodeIds(fixture));
    assertGraphIsTheFixtureGraph(NodeGraphFile.load(PORT, null), fixtureNodeIds(fixture));
  }

  /** The graph is soft state: an unreadable file means an empty graph, not a failed start. */
  @Test
  void corruptGraphFile_startsWithAnEmptyGraph() throws Exception {
    Files.writeString(NodeGraphFile.file(PORT).toPath(), "{\"format\":\"redpanda-node-gr");

    assertThat(NodeGraphFile.load(PORT, new LocalSettings()).vertexSet()).isEmpty();
  }

  @Test
  void missingGraphFile_startsWithAnEmptyGraph() {
    assertThat(NodeGraphFile.load(PORT, new LocalSettings()).vertexSet()).isEmpty();
  }

  /** No FQCN may be pinned in the file (T117 lesson), and the header names the format. */
  @Test
  void graphFileIsHeaderedJson() throws Exception {
    NodeGraphFile.save(
        PORT, new DefaultDirectedWeightedGraph<>(NodeEdge.class), null, new LocalSettings());

    assertThat(Files.readString(NodeGraphFile.file(PORT).toPath()))
        .startsWith("{\"format\":\"redpanda-node-graph\",\"version\":1,\"nodeGraph\":")
        .doesNotContain("im.redpanda");
  }

  private static ServerContext contextFor(LocalSettings settings) {
    ServerContext serverContext = new ServerContext();
    serverContext.setPort(PORT);
    serverContext.setLocalSettings(settings);
    serverContext.setNodeId(settings.getMyIdentity());
    return serverContext;
  }

  private static void assertGraphIsTheFixtureGraph(
      DefaultDirectedWeightedGraph<Node, NodeEdge> graph, Set<KademliaId> expectedNodes) {
    assertThat(graph.vertexSet().stream().map(Node::getNodeId).map(NodeId::getKademliaId))
        .containsExactlyInAnyOrderElementsOf(expectedNodes);
    assertThat(graph.edgeSet()).hasSize(1);
    assertThat(graph.getEdgeWeight(graph.edgeSet().iterator().next())).isEqualTo(17d);
  }

  private byte[] readFixture() throws IOException {
    try (InputStream in = getClass().getResourceAsStream(PRE_T136_FIXTURE)) {
      return in.readAllBytes();
    }
  }

  private static KademliaId fixtureIdentity(byte[] fixture) throws IOException {
    JsonObject json = JsonParser.parseString(new String(fixture)).getAsJsonObject();
    return NodeIdCodec.nodeIdFromJson(json.getAsJsonObject("identity")).getKademliaId();
  }

  private static Set<KademliaId> fixtureNodeIds(byte[] fixture) throws IOException {
    JsonObject json = JsonParser.parseString(new String(fixture)).getAsJsonObject();
    return NodeGraphCodec.fromJson(json.getAsJsonObject("nodeGraph")).vertexSet().stream()
        .map(Node::getNodeId)
        .map(NodeId::getKademliaId)
        .collect(Collectors.toSet());
  }
}
