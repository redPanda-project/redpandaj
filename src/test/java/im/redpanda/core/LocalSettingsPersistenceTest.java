package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import im.redpanda.identity.NodeId;
import im.redpanda.ops.Settings;
import im.redpanda.routing.graph.Node;
import im.redpanda.routing.graph.NodeEdge;
import java.io.File;
import java.nio.file.Files;
import java.security.Security;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class LocalSettingsPersistenceTest {

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  private int port;

  @BeforeEach
  void setUp() {
    // Use a unique, unlikely port to avoid clobbering other tests/files
    port = 49123;
    // Cleanup pre-existing files if any
    deleteSettingsFiles();
  }

  @AfterEach
  void tearDown() {
    deleteSettingsFiles();
  }

  private void deleteSettingsFiles() {
    // best effort
    settingsFile().delete();
    tmpSettingsFile().delete();
    legacySettingsFile().delete();
  }

  private File settingsFile() {
    return LocalSettings.settingsFile(port);
  }

  private File tmpSettingsFile() {
    return LocalSettings.tmpSettingsFile(port);
  }

  private File legacySettingsFile() {
    return LocalSettings.legacySettingsFile(port);
  }

  @Test
  void saveAndLoadRoundtrip() {
    LocalSettings ls = new LocalSettings();
    ls.setUpdateTimestamp(123456789L);
    byte[] sig = new byte[] {1, 2, 3, 4};
    ls.setUpdateSignature(sig);
    ls.setUpdateAndroidTimestamp(222L);
    byte[] asig = new byte[] {9, 8, 7};
    ls.setUpdateAndroidSignature(asig);

    ls.save(port);

    LocalSettings loaded = LocalSettings.load(port);
    assertNotNull(loaded);
    assertThat(loaded.getUpdateTimestamp()).isEqualTo(123456789L);
    assertArrayEquals(sig, loaded.getUpdateSignature());
    assertThat(loaded.getUpdateAndroidTimestamp()).isEqualTo(222L);
    assertArrayEquals(asig, loaded.getUpdateAndroidSignature());
    assertNotNull(loaded.getMyIdentity());
    assertNotNull(loaded.getNodeGraph());
    assertNotNull(loaded.getSystemUpTimeData());
    assertThat(tmpSettingsFile()).doesNotExist();
  }

  /**
   * Regression test for REDPANDAJ-2E6: a save that blows up half way through serialization (there:
   * a ConcurrentModificationException on a collection another thread was mutating) must not destroy
   * the settings file that is already on disk — it holds the node identity.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  void failedSaveKeepsPreviousFile() throws Exception {
    LocalSettings ls = new LocalSettings();
    ls.setUpdateTimestamp(4711L);
    ls.save(port);

    byte[] savedFile = Files.readAllBytes(settingsFile().toPath());

    // a vertex that is not a Node makes the encoder fail in the middle of the graph, just like
    // the ConcurrentModificationException did (T117: unchecked now, hence the RuntimeException
    // catch in save())
    ((DefaultDirectedWeightedGraph) ls.getNodeGraph()).addVertex(new Object());
    ls.setUpdateTimestamp(999L);

    ls.save(port);

    assertThat(LocalSettings.load(port).getUpdateTimestamp()).isEqualTo(4711L);
    // asserted as a boolean, an array comparison would dump both files into the failure message
    assertThat(Arrays.equals(savedFile, Files.readAllBytes(settingsFile().toPath())))
        .as("the settings file on disk must be byte identical to the last successful save")
        .isTrue();
    assertThat(tmpSettingsFile()).doesNotExist();
  }

  /**
   * SaveJobs and the update handling in InboundCommandProcessor both call save(), and both saves
   * write the same file (and the same temporary file). save() therefore has to hold the monitor of
   * the settings instance for the whole write.
   */
  @Test
  @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
  void savesExcludeEachOther() throws Exception {
    LocalSettings ls = new LocalSettings();
    ls.setUpdateTimestamp(4711L);

    CountDownLatch saveStarted = new CountDownLatch(1);
    CountDownLatch saveFinished = new CountDownLatch(1);
    Thread saver =
        new Thread(
            () -> {
              saveStarted.countDown();
              ls.save(port);
              saveFinished.countDown();
            });

    synchronized (ls) {
      saver.start();
      assertThat(saveStarted.await(30, TimeUnit.SECONDS)).isTrue();
      // a save that does not take the monitor would be through in well under a second
      assertThat(saveFinished.await(1, TimeUnit.SECONDS))
          .as("save() must not write while another thread holds the settings monitor")
          .isFalse();
    }

    assertThat(saveFinished.await(30, TimeUnit.SECONDS)).isTrue();
    saver.join();
    assertThat(LocalSettings.load(port).getUpdateTimestamp()).isEqualTo(4711L);
    assertThat(tmpSettingsFile()).doesNotExist();
  }

  /**
   * The serialized node graph is the very object NodeStore mutates under its write lock (see the
   * REDPANDAJ-2DW comment in NodeStore#maintainNodes). Serializing it without the matching read
   * lock risks a ConcurrentModificationException; since #282 that no longer truncates the file, but
   * the save fails and the graph never reaches the disk.
   *
   * <p>Asserted via the lock itself instead of a racing stress test: a save that takes the read
   * lock cannot make progress while the write lock is held. That is deterministic and one-sided —
   * see {@link ConcurrencyTestSupport}.
   */
  @Test
  @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
  void saveBlocksWhileTheNodeGraphWriteLockIsHeld() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    LocalSettings localSettings = serverContext.getLocalSettings();
    localSettings.setUpdateTimestamp(4711L);

    // buildDefaultServerContext builds the NodeStore, which is where the graph and its lock are
    // handed over - so this also covers the wiring, not just LocalSettings itself.
    ConcurrencyTestSupport.assertBlockedWhileHeld(
        serverContext.getNodeStore().getReadWriteLock().writeLock(),
        () -> localSettings.save(port));

    assertThat(LocalSettings.load(port).getUpdateTimestamp()).isEqualTo(4711L);
    assertThat(tmpSettingsFile()).doesNotExist();
  }

  /** A LocalSettings that no NodeStore has adopted (e.g. Updater) still has to save. */
  @Test
  @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
  void saveWorksWithoutANodeGraphLock() {
    LocalSettings localSettings = new LocalSettings();
    localSettings.setUpdateTimestamp(1234L);

    localSettings.save(port);

    assertThat(LocalSettings.load(port).getUpdateTimestamp()).isEqualTo(1234L);
    assertThat(tmpSettingsFile()).doesNotExist();
  }

  /**
   * T117: the identity, the updater timestamps and the node graph must survive a full save/load
   * cycle in the explicit JSON format — the identity byte for byte, because it is the node's
   * Kademlia standing.
   */
  @Test
  void roundtripKeepsIdentityGraphAndUptime() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    LocalSettings ls = serverContext.getLocalSettings();
    ls.setUpdateTimestamp(1783728000001L);
    ls.getSystemUpTimeData().reportNow();

    // two nodes, one edge - the edge state has to come back as it was, not as "checked just now"
    Node a = new Node(serverContext, new NodeId());
    a.seen("10.0.0.1", 59558);
    a.setGmTestsSuccessful(7);
    Node b = new Node(serverContext, new NodeId());
    ls.getNodeGraph().addVertex(a);
    ls.getNodeGraph().addVertex(b);
    NodeEdge edge = ls.getNodeGraph().addEdge(a, b);
    ls.getNodeGraph().setEdgeWeight(edge, 17d);
    edge.setLastCheckFailed(true);
    long timeLastCheckFailed = edge.getTimeLastCheckFailed();

    ls.save(port);
    LocalSettings loaded = LocalSettings.load(port);

    assertArrayEquals(
        ls.getMyIdentity().exportWithPrivate(), loaded.getMyIdentity().exportWithPrivate());
    assertThat(loaded.getMyIdentity().getKademliaId())
        .isEqualTo(ls.getMyIdentity().getKademliaId());
    assertThat(loaded.getUpdateTimestamp()).isEqualTo(1783728000001L);
    assertThat(loaded.getSystemUpTimeData().getUptimePercent())
        .isEqualTo(ls.getSystemUpTimeData().getUptimePercent());

    DefaultDirectedWeightedGraph<Node, NodeEdge> graph = loaded.getNodeGraph();
    assertThat(graph.vertexSet()).hasSize(2);
    Node loadedA = graph.vertexSet().stream().filter(n -> n.equals(a)).findFirst().orElseThrow();
    assertThat(loadedA.getGmTestsSuccessful()).isEqualTo(7);
    assertThat(loadedA.latestSeenConnectionPoint().getIp()).isEqualTo("10.0.0.1");
    assertThat(loadedA.latestSeenConnectionPoint().getPort()).isEqualTo(59558);
    assertThat(graph.edgeSet()).hasSize(1);
    NodeEdge loadedEdge = graph.edgeSet().iterator().next();
    assertThat(graph.getEdgeWeight(loadedEdge)).isEqualTo(17d);
    assertThat(loadedEdge.isLastCheckFailed()).isTrue();
    assertThat(loadedEdge.getTimeLastCheckFailed()).isEqualTo(timeLastCheckFailed);
  }

  /**
   * A node that shares an edge with two others must come back as ONE object, not one copy per edge:
   * the graph is keyed on Node identity/equality and NodeStore mutates the vertices in place.
   */
  @Test
  void roundtripKeepsSharedVertexInstances() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    LocalSettings ls = serverContext.getLocalSettings();
    Node hub = new Node(serverContext, new NodeId());
    Node left = new Node(serverContext, new NodeId());
    Node right = new Node(serverContext, new NodeId());
    for (Node node : new Node[] {hub, left, right}) {
      ls.getNodeGraph().addVertex(node);
    }
    ls.getNodeGraph().addEdge(left, hub);
    ls.getNodeGraph().addEdge(hub, right);

    ls.save(port);
    DefaultDirectedWeightedGraph<Node, NodeEdge> graph = LocalSettings.load(port).getNodeGraph();

    assertThat(graph.vertexSet()).hasSize(3);
    NodeEdge toHub =
        graph.edgeSet().stream()
            .filter(e -> graph.getEdgeTarget(e).equals(hub))
            .findFirst()
            .orElseThrow();
    NodeEdge fromHub =
        graph.edgeSet().stream()
            .filter(e -> graph.getEdgeSource(e).equals(hub))
            .findFirst()
            .orElseThrow();
    assertThat(graph.getEdgeTarget(toHub)).isSameAs(graph.getEdgeSource(fromHub));
  }

  /**
   * T117 / user decision 2026-09-01: there is no migration path. A node that only finds the
   * pre-T117 Java-serialized file starts with a FRESH identity, warns naming that file, and leaves
   * it on disk.
   */
  @Test
  void legacyDatFileIsNotReadAndNotDeleted() throws Exception {
    new File(Settings.SAVE_DIR).mkdir();
    Files.write(legacySettingsFile().toPath(), new byte[] {(byte) 0xac, (byte) 0xed, 0, 5});

    LocalSettings loaded = LocalSettings.load(port);

    assertNotNull(loaded.getMyIdentity());
    assertThat(loaded.getMyIdentity().hasPrivate()).isTrue();
    assertThat(loaded.getUpdateTimestamp()).isEqualTo(-1L);
    assertThat(legacySettingsFile()).as("the legacy file must be kept, not deleted").exists();
    assertThat(settingsFile()).as("fresh settings are written in the new format").exists();
  }

  /** A corrupt settings file must regenerate the identity instead of throwing, and be kept. */
  @Test
  void corruptSettingsFileRegeneratesAndIsKept() throws Exception {
    LocalSettings ls = new LocalSettings();
    ls.setUpdateTimestamp(4711L);
    ls.save(port);

    Files.writeString(settingsFile().toPath(), "{\"format\":\"redpanda-local-settings\",\"ver");

    LocalSettings loaded = LocalSettings.load(port);

    assertThat(loaded.getMyIdentity().hasPrivate()).isTrue();
    assertThat(loaded.getMyIdentity().getKademliaId())
        .as("a fresh identity, the old one is unreadable")
        .isNotEqualTo(ls.getMyIdentity().getKademliaId());
    assertThat(loaded.getUpdateTimestamp()).isEqualTo(-1L);
  }

  /** A settings file of a future schema version is treated like a corrupt one. */
  @Test
  void unknownFormatVersionRegenerates() throws Exception {
    LocalSettings ls = new LocalSettings();
    ls.save(port);
    String json = Files.readString(settingsFile().toPath());
    Files.writeString(settingsFile().toPath(), json.replace("\"version\":1", "\"version\":99"));

    assertThat(LocalSettings.load(port).getMyIdentity().getKademliaId())
        .isNotEqualTo(ls.getMyIdentity().getKademliaId());
  }

  /**
   * The deploy path: {@code Updater.insertNewUpdate} loads the settings of port 59558, stamps the
   * signature and timestamp of the freshly built jar into them and saves. If that roundtrip loses
   * the signature, the auto-updater serves nothing and the testnet cannot be deployed at all.
   */
  @Test
  void updaterSignatureRoundtrip() {
    byte[] signature = new byte[NodeId.SIGNATURE_LEN];
    for (int i = 0; i < signature.length; i++) {
      signature[i] = (byte) i;
    }
    byte[] apkSignature = new byte[NodeId.SIGNATURE_LEN];
    java.util.Arrays.fill(apkSignature, (byte) 0x5a);

    LocalSettings localSettings = LocalSettings.load(port);
    localSettings.setUpdateSignature(signature);
    localSettings.setUpdateTimestamp(1783728000042L);
    localSettings.setUpdateAndroidSignature(apkSignature);
    localSettings.setUpdateAndroidTimestamp(1783728000043L);
    localSettings.save(port);

    LocalSettings reloaded = LocalSettings.load(port);
    assertArrayEquals(signature, reloaded.getUpdateSignature());
    assertThat(reloaded.getUpdateTimestamp()).isEqualTo(1783728000042L);
    assertArrayEquals(apkSignature, reloaded.getUpdateAndroidSignature());
    assertThat(reloaded.getUpdateAndroidTimestamp()).isEqualTo(1783728000043L);
    assertThat(reloaded.getMyIdentity().getKademliaId())
        .as("stamping an update must not rotate the identity")
        .isEqualTo(localSettings.getMyIdentity().getKademliaId());
  }

  /** No node state may be Java-serialized any more (DDD review §5, T117). */
  @Test
  void settingsFileIsJsonNotAJavaObjectStream() throws Exception {
    new LocalSettings().save(port);

    byte[] written = Files.readAllBytes(settingsFile().toPath());

    assertThat(written[0]).as("Java object streams start with 0xACED").isNotEqualTo((byte) 0xac);
    assertThat(new String(written, java.nio.charset.StandardCharsets.UTF_8))
        .startsWith("{\"format\":\"redpanda-local-settings\",\"version\":1")
        .as("no fully qualified class name may be pinned in the file")
        .doesNotContain("im.redpanda");
  }

  /**
   * Copilot review of #337: a header whose {@code version} is not a number must read as "unreadable
   * file", not as an unchecked exception escaping a method that declares IOException.
   */
  @Test
  void nonNumericFormatVersionRegenerates() throws Exception {
    LocalSettings ls = new LocalSettings();
    ls.save(port);
    String json = Files.readString(settingsFile().toPath());
    Files.writeString(settingsFile().toPath(), json.replace("\"version\":1", "\"version\":{}"));

    assertThat(LocalSettings.load(port).getMyIdentity().getKademliaId())
        .isNotEqualTo(ls.getMyIdentity().getKademliaId());
  }
}
