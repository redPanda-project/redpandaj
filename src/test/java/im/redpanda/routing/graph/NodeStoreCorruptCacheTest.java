package im.redpanda.routing.graph;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.LocalSettings;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

/**
 * TD159: {@code NodeStore.get()} used to swallow every exception with {@code e.printStackTrace()}
 * and then wipe the whole on-disk tier. With T117's explicit serializers a throw out of that read
 * is a precise "the cached entry cannot be deserialized" signal, so it has to be visible (WARN +
 * Sentry) — and the recovery must not itself blow up.
 *
 * <p>The corruption here is the realistic one: a node cache written with a different value
 * serializer, i.e. a file from an incompatible build. The on-heap tier is empty on start, so the
 * read cascades through the off-heap tier down to the file and fails in {@code
 * NodeStoreSerializers.NODE}.
 *
 * <p>The port is unique to this test class: the cache file name is derived from it and the surefire
 * forks share a working directory (see the T70 fork-CWD collision).
 */
class NodeStoreCorruptCacheTest {

  private static final int PORT = 59711;

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  private final Path cachePath = Path.of(NodeStore.nodeCachePath(PORT));
  private NodeStore nodeStore;

  private final List<LogEvent> events = new CopyOnWriteArrayList<>();
  private LoggerContext context;
  private CapturingAppender appender;

  private final class CapturingAppender extends AbstractAppender {
    private CapturingAppender() {
      super("NodeStoreCorruptCacheTestAppender", null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }
  }

  @BeforeEach
  void cleanCache() throws IOException {
    Files.createDirectories(cachePath.getParent());
    Files.deleteIfExists(cachePath);

    context = (LoggerContext) LogManager.getContext(false);
    appender = new CapturingAppender();
    appender.start();
    LoggerConfig loggerConfig = new LoggerConfig(NodeStore.class.getName(), Level.ALL, false);
    loggerConfig.addAppender(appender, Level.ALL, null);
    context.getConfiguration().addLogger(NodeStore.class.getName(), loggerConfig);
    context.updateLoggers();
  }

  @AfterEach
  void tearDown() throws IOException {
    context.getConfiguration().removeLogger(NodeStore.class.getName());
    context.updateLoggers();
    appender.stop();
    closeStore();
    Files.deleteIfExists(cachePath);
  }

  /**
   * Closes the store and puts a fresh expire executor back.
   *
   * <p>MapDB's {@code HTreeMap.close()} shuts down the executor it was given, and every cache tier
   * here is handed the JVM-wide static {@code NodeStore.threadPool}. Closing one store therefore
   * terminates that pool for the whole JVM, and every later {@code NodeStore} build in the same
   * surefire fork fails with a {@code RejectedExecutionException}. Replacing it keeps this test
   * from poisoning its neighbours; the production side of that landmine is logged as tech debt.
   */
  private void closeStore() {
    if (nodeStore == null) {
      return;
    }
    nodeStore.close();
    nodeStore = null;
    NodeStore.threadPool = Executors.newScheduledThreadPool(2);
  }

  @Test
  void get_onCorruptDiskCache_returnsNullAndClearsTheDiskTier() throws IOException {
    KademliaId id = new NodeId().getKademliaId();
    writeUnreadableEntry(id);

    ServerContext serverContext = new ServerContext();
    serverContext.setPort(PORT);
    serverContext.setLocalSettings(new LocalSettings());
    nodeStore = NodeStore.buildWithDiskCache(serverContext);
    serverContext.setNodeStore(nodeStore);

    // Must not propagate: a corrupt cache is recoverable, the graph is rebuilt from the network.
    assertThat(nodeStore.get(id)).isNull();

    // The whole point of TD159: the failure is reported instead of printed to stdout, and it
    // carries the cause so the "corrupt cache" diagnosis survives into the log and into Sentry.
    assertThat(events)
        .anySatisfy(
            event -> {
              assertThat(event.getLevel()).isEqualTo(Level.WARN);
              assertThat(event.getMessage().getFormattedMessage()).contains("could not read node");
              assertThat(event.getThrown()).isNotNull();
            });

    // ... and the unreadable entry is gone, so the next read is a plain miss instead of a
    // permanent exception on every lookup. Reading the file back is what proves the catch block
    // ran at all: the entry can only disappear through the onDisk.clear() in there.
    closeStore();
    assertThat(rawEntryCount()).isZero();
  }

  /**
   * A memory-only store has no on-disk tier at all. The old recovery path dereferenced {@code
   * onDisk} unconditionally, so any failure there would have turned into an NPE thrown out of the
   * catch block.
   */
  @Test
  void get_onMemoryOnlyStore_hasNoDiskTierToClear() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    Node node = new Node(serverContext, new NodeId());

    assertThat(serverContext.getNodeStore().get(node.getNodeId().getKademliaId())).isSameAs(node);
    assertThat(serverContext.getNodeStore().get(new NodeId().getKademliaId())).isNull();
  }

  /**
   * Writes the node map with {@link Serializer#BYTE_ARRAY} as the value serializer. The bytes are
   * not JSON, so {@code NodeStoreSerializers.NODE} throws while reading them back.
   */
  private void writeUnreadableEntry(KademliaId id) throws IOException {
    try (DB db = DBMaker.fileDB(cachePath.toFile()).checksumHeaderBypass().make()) {
      db.hashMap(NodeStore.NODE_MAP, NodeStoreSerializers.KADEMLIA_ID, Serializer.BYTE_ARRAY)
          .createOrOpen()
          .put(id, new byte[] {0x7f, 0x00, 0x11, 0x22});
    }
    assertThat(rawEntryCount()).isEqualTo(1);
  }

  /** Entries in the node map of the cache file. Only callable while no store holds the file. */
  private int rawEntryCount() {
    try (DB db = DBMaker.fileDB(cachePath.toFile()).checksumHeaderBypass().make()) {
      return db.hashMap(NodeStore.NODE_MAP, NodeStoreSerializers.KADEMLIA_ID, Serializer.BYTE_ARRAY)
          .createOrOpen()
          .size();
    }
  }
}
