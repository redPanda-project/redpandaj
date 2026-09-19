package im.redpanda.routing.graph;

import im.redpanda.core.LocalSettings;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.ops.Log;
import im.redpanda.routing.PeerPerformanceTestGarlicMessageJob;
import im.redpanda.transport.Peer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public class NodeStore {

  private static final Logger logger = LogManager.getLogger();

  /** Name of the node map in every cache tier; "V2" is the T117 format (explicit serializers). */
  static final String NODE_MAP = "nodesV2";

  public static final long NODE_BLACKLISTED_FOR_GRAPH = 1000L * 60L * 60L * 24L;
  public static final int MAX_EDGES_IN_GRAPH = 500;
  public static final int MIN_EDGES_NEEDED_FOR_NODE_REMOVAL = 5;
  public static final int MAX_NODES_FOR_GRAPH = 20;

  /**
   * The expiry executor of this store's cache tiers.
   *
   * <p>Per instance, not JVM-wide (TD184). MapDB's {@code HTreeMap.close()} shuts the executor it
   * was handed down, so a single shared pool meant that closing <em>one</em> store — which {@link
   * #saveToDisk()}'s recovery path does — terminated the expiry threads of every store built
   * afterwards in the same JVM, with a {@code RejectedExecutionException} out of the next {@code
   * createOrOpen()}. Ownership now matches lifetime: the store that creates the pool is the store
   * that closes it.
   */
  private final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(2);

  /**
   * These sizes are upper limits of the different dbs, the main eviction should be done via a
   * timeout after a get operation since the eviction by size is random.
   */
  private static final long MAX_SIZE_ONHEAP = 50L * 1024L * 1024L;

  private static final long MAX_SIZE_OFFHEAP = 50L * 1024L * 1024L;
  private static final long MAX_SIZE_ONDISK = 300L * 1024L * 1024L;

  private HTreeMap<KademliaId, Node> onHeap;
  private HTreeMap<KademliaId, Node> offHeap;
  private HTreeMap<KademliaId, Node> onDisk;
  private DB dbonHeap;
  private DB dboffHeap;
  private DB dbDisk;

  private DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph;
  private long lastTimeEdgeAdded = 0;
  private final ServerContext serverContext;
  private final SecureRandom random = new SecureRandom();

  /**
   * Guards {@link #nodeGraph}. Not final: a store built to replace a broken one takes over the lock
   * (and the graph) of its predecessor, see {@link #buildWithDiskCache(ServerContext, NodeStore)}.
   */
  @Getter private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  /**
   * Serializes {@link #saveToDisk()} against {@link #close()}.
   *
   * <p>Both run on the job pool, and by construction at the same instant: {@code ServerRestartJob}
   * ticks hourly and {@code SaveJobs} every 15 minutes, both counted from process start, so every
   * restart tick is also a save tick. {@code Server.shutdown()} then closed the tiers while the
   * save was still inside {@code clearWithExpire()}; MapDB threw {@code IllegalAccessError: Store
   * was closed}, and the recovery path below read that as a corrupt cache — deleted the file and
   * rebuilt the store a moment before the JVM exited (REDPANDAJ-2EZ, testnet node1 restart
   * 2026-09-05 01:46 UTC). With the lock, close waits for an in-flight save and a save after close
   * is a no-op.
   */
  private final Object lifecycleLock = new Object();

  /**
   * Volatile, because {@link #liveStore()} reads it <em>outside</em> {@link #lifecycleLock} on the
   * cache hot path — see the note there.
   */
  private volatile boolean closed;

  /**
   * One report per store for the "closed and never replaced" case, see {@link
   * #reportNoLiveStore()}.
   */
  private final AtomicBoolean reportedNoLiveStore = new AtomicBoolean();

  /**
   * Test seam: runs between the flush steps of {@link #saveToDiskLocked()}, i.e. while the save
   * holds {@link #lifecycleLock}. Null outside tests. Lets {@code NodeStoreShutdownRaceTest} park a
   * close() on the lock deterministically instead of hoping a sleep lines up.
   */
  static volatile Runnable betweenFlushStepsForTest;

  private NodeStore(ServerContext serverContext) {
    this.serverContext = serverContext;
    nodeGraph = new DefaultDirectedWeightedGraph<>(NodeEdge.class);
  }

  public static NodeStore buildWithDiskCache(ServerContext serverContext) {
    return buildWithDiskCache(serverContext, null);
  }

  /**
   * @param replacing the store this one takes over from ({@link #saveToDisk()}'s recovery), or
   *     {@code null} for a fresh start. See {@link #takeOverGraphGuardFrom(NodeStore)}.
   */
  private static NodeStore buildWithDiskCache(ServerContext serverContext, NodeStore replacing) {

    NodeStore nodeStore = new NodeStore(serverContext);

    if (replacing != null) {
      nodeStore.takeOverGraphGuardFrom(replacing);
    } else if (serverContext.getLocalSettings() == null) {
      logger.warn("could not restore nodeGraph from local settings, starting with an empty graph");
    } else {
      nodeStore.adoptGraphOf(serverContext.getLocalSettings());
    }

    try {
      buildDiskTiers(serverContext, nodeStore);
    } catch (RuntimeException | Error e) {
      // The executor is created with the instance, i.e. before any tier exists, and MapDB opens
      // real heap/direct-memory/file handles here. Dropping a half-built store on the floor would
      // leak two live threads and those handles for the rest of the process (Sonnet review, T150).
      nodeStore.close();
      throw e;
    }

    return nodeStore;
  }

  private static void buildDiskTiers(ServerContext serverContext, NodeStore nodeStore) {
    nodeStore.dbonHeap =
        DBMaker.heapDB()
            // .closeOnJvmShutdown()
            .make();

    nodeStore.dboffHeap =
        DBMaker.memoryDirectDB()
            // .closeOnJvmShutdown()
            .make();

    logStaleLegacyCache(serverContext.getPort());

    nodeStore.dbDisk =
        DBMaker.fileDB(nodeCachePath(serverContext.getPort()))
            .fileMmapEnableIfSupported()
            // .closeOnJvmShutdown()
            .checksumHeaderBypass()
            .make();

    nodeStore.onDisk =
        nodeStore
            .dbDisk
            .hashMap(NODE_MAP, NodeStoreSerializers.KADEMLIA_ID, NodeStoreSerializers.NODE)
            .expireStoreSize(MAX_SIZE_ONDISK)
            .expireExecutor(nodeStore.threadPool)
            // .expireAfterUpdate(60, TimeUnit.SECONDS) // no update since 14 days,
            // not seen in this time
            .expireAfterGet(60, TimeUnit.DAYS)
            .createOrOpen();

    nodeStore.offHeap =
        nodeStore
            .dboffHeap
            .hashMap(NODE_MAP, NodeStoreSerializers.KADEMLIA_ID, NodeStoreSerializers.NODE)
            .expireStoreSize(MAX_SIZE_OFFHEAP)
            .expireOverflow(nodeStore.onDisk)
            .expireExecutor(nodeStore.threadPool)
            .expireAfterCreate()
            .expireAfterGet(60, TimeUnit.MINUTES)
            .create();

    nodeStore.onHeap =
        nodeStore
            .dbonHeap
            .hashMap(NODE_MAP, NodeStoreSerializers.KADEMLIA_ID, NodeStoreSerializers.NODE)
            .expireStoreSize(MAX_SIZE_ONHEAP)
            .expireOverflow(nodeStore.offHeap)
            .expireExecutor(nodeStore.threadPool)
            .expireAfterCreate()
            .expireAfterGet(15, TimeUnit.MINUTES)
            .create();
  }

  public static NodeStore buildWithMemoryCacheOnly(ServerContext serverContext) {
    return buildWithMemoryCacheOnly(serverContext, null);
  }

  private static NodeStore buildWithMemoryCacheOnly(
      ServerContext serverContext, NodeStore replacing) {
    NodeStore nodeStore = new NodeStore(serverContext);

    if (replacing != null) {
      nodeStore.takeOverGraphGuardFrom(replacing);
    } else if (serverContext.getLocalSettings() == null) {
      Log.put("warning, could not restore nodeGraph from local settings....", 5);
    } else {
      nodeStore.adoptGraphOf(serverContext.getLocalSettings());
    }

    try {
      nodeStore.dbonHeap = DBMaker.heapDB().make();

      nodeStore.onHeap =
          nodeStore
              .dbonHeap
              .hashMap(NODE_MAP, NodeStoreSerializers.KADEMLIA_ID, NodeStoreSerializers.NODE)
              .expireStoreSize(MAX_SIZE_ONHEAP)
              .expireExecutor(nodeStore.threadPool)
              .expireAfterCreate()
              .expireAfterGet(15, TimeUnit.HOURS)
              .create();
    } catch (RuntimeException | Error e) {
      nodeStore.close();
      throw e;
    }

    return nodeStore;
  }

  /**
   * Makes this store the successor of {@code previous}: same graph object, same lock object.
   *
   * <p>The lock matters more than the graph. {@code LocalSettings} holds the read lock it was
   * handed at startup and serializes the graph under it, while jobs that cached a {@code NodeStore}
   * reference mutate the very same graph under their store's write lock. Handing {@code
   * LocalSettings} a <em>different</em> lock mid-flight would leave a mutator and the serializer
   * holding two unrelated locks over one graph — so the successor keeps the predecessor's lock
   * instead, and no re-registration happens at all (Sonnet review, T150). It is also why the
   * recovery does not start from an empty graph: the vertices the DHT jobs hold references to must
   * stay in it ("no such vertex in graph", deploy #9).
   */
  private void takeOverGraphGuardFrom(NodeStore previous) {
    this.readWriteLock = previous.readWriteLock;
    this.nodeGraph = previous.nodeGraph;
  }

  /**
   * Takes over the persisted graph of {@code localSettings} as the live graph and hands the
   * settings the read lock that guards it. From here on both sides agree on how the graph is
   * protected: this store mutates it under {@link #readWriteLock}'s write lock (REDPANDAJ-2DW) and
   * {@code LocalSettings.save()} serializes it under the read lock.
   */
  private void adoptGraphOf(LocalSettings localSettings) {
    nodeGraph = localSettings.getNodeGraph();
    localSettings.setNodeGraphLock(readWriteLock.readLock());
  }

  /**
   * The store a caller should actually work on: {@code this} while it is open, otherwise the
   * successor that {@link #saveToDisk()}'s recovery installed in the {@link ServerContext}.
   *
   * <p>TD215: the store swap in the recovery path is not atomic with respect to threads that
   * already hold a {@code NodeStore} reference. {@code Node}'s constructor re-reads {@code
   * serverContext.getNodeStore()} for every access, but {@code PeerPerformanceTestGarlicMessageJob}
   * deliberately resolves the store <em>once</em> (it must lock and unlock the same lock object),
   * and any future caller may do the same. Those references keep pointing at the closed
   * predecessor, whose MapDB tiers throw {@code IllegalAccessError: Store was closed} on the next
   * access — {@link #get(KademliaId)} turned that into a spurious "corrupt cache" diagnosis plus an
   * {@code onDisk.clear()} on a closed tier, and {@link #put(KademliaId, Node)} propagated it into
   * {@code new Node(...)}, i.e. onto the inbound-connection path.
   *
   * <p><b>Deliberately lock-free on the fast path.</b> {@link #lifecycleLock} is held by {@link
   * #saveToDisk()} for the whole flush, and on the recovery path also across {@code close()}, the
   * cache-file delete and a full three-tier rebuild including the mmap. {@code get()}/{@code put()}
   * run on the NIO selector thread ({@code ConnectionHandler.setupConnection} → {@code
   * Node.getByKademliaId} → {@code new Node(...)}) and on the reader threads, so reading {@link
   * #closed} under that lock would stall every inbound handshake for the duration of a flush (T149
   * review finding). Hence the volatile read.
   *
   * <p>The one case that does wait is a store that is closed but <em>still</em> installed in the
   * context: that is either shutdown (no successor is coming) or the window inside the recovery
   * between {@code close()} and {@code setNodeStore()}, and telling the two apart means waiting for
   * the very lock the recovery holds. Rare by construction and never the hot path.
   *
   * <p>Exactly one hop, never back to this instance. A successor that is itself already closed (two
   * failing flushes in a row) is answered with {@code null} rather than chased further.
   *
   * <p>Residual check-then-act window: a store that is open when it is returned here can be closed
   * before the caller reaches its tiers. The three accessors below catch MapDB's {@code
   * IllegalAccessError} for exactly that case and retry once.
   *
   * @return the live store, or {@code null} when there is none — then a cache write is dropped and
   *     a read answers {@code null}.
   */
  private NodeStore liveStore() {
    if (!closed) {
      return this;
    }
    NodeStore live = serverContext.getNodeStore();
    if (live == this) {
      synchronized (lifecycleLock) {
        live = serverContext.getNodeStore();
      }
    }
    if (live == null || live == this || live.closed) {
      return null;
    }
    return live;
  }

  /**
   * A cache access found no open store at all.
   *
   * <p>During shutdown that is expected — job and reader threads keep running for a moment after
   * {@code Server.shutdown()} closed the store — so it stays a debug line. Outside shutdown it
   * means the recovery's file-backed rebuild <em>and</em> its memory-only fallback both threw: the
   * node then silently treats every peer as unknown for the rest of the process, and the only trace
   * used to be the one earlier exception (T149 review finding). Reported once per store instance so
   * a busy node neither spams the log nor Sentry.
   */
  private void reportNoLiveStore() {
    if (Server.isShuttingDown()) {
      logger.debug("node cache access after shutdown closed the store, dropping it");
      return;
    }
    if (reportedNoLiveStore.compareAndSet(false, true)) {
      logger.warn("the node store is closed and was not replaced, every cache access is a no-op");
      Log.sentry("node store closed without a replacement, every cache access is dropped");
    }
  }

  public void put(KademliaId kademliaId, Node node) {
    NodeStore live = liveStore();
    if (live == null) {
      reportNoLiveStore();
      return;
    }
    try {
      live.onHeap.put(kademliaId, node);
    } catch (IllegalAccessError closedUnderUs) {
      // "Store was closed": the store was open when liveStore() resolved it and was closed before
      // this line ran. See liveStore()'s note on the check-then-act window. Exactly one retry —
      // the close has happened by now, so this resolves to the successor.
      NodeStore retry = live.liveStore();
      if (retry == null) {
        reportNoLiveStore();
        return;
      }
      retry.onHeap.put(kademliaId, node);
    }
  }

  /**
   * Reads a node from the cache; a miss on the on-heap tier cascades down to the off-heap and
   * on-disk tiers via MapDB's overflow loader.
   *
   * <p>TD159: since T117 the tiers have explicit serializers, so an exception out of this read is a
   * precise signal — one of the cached entries cannot be deserialized, i.e. the on-disk cache is
   * corrupt or was written by an incompatible build. Dropping the disk tier is the right recovery
   * (the graph is rebuilt from the network, user decision 2026-09-01), but it used to happen behind
   * an {@code e.printStackTrace()}: a node threw its whole node cache away with no line in {@code
   * redpanda.log} and nothing in Sentry. Now it is a WARN plus a Sentry event, and the clear is
   * guarded — {@link #buildWithMemoryCacheOnly(ServerContext)} leaves {@code onDisk} null, so the
   * old code would have NPEd inside the catch block instead of returning null.
   */
  public Node get(KademliaId kademliaId) {
    NodeStore live = liveStore();
    if (live == null) {
      reportNoLiveStore();
      return null;
    }
    try {
      return live.onHeap.get(kademliaId);
    } catch (IllegalAccessError closedUnderUs) {
      // Closed between liveStore() and this line, see put(). Not a corrupt cache — do NOT fall
      // into the branch below, which would clear a perfectly good on-disk tier.
      NodeStore retry = live.liveStore();
      if (retry == null) {
        reportNoLiveStore();
        return null;
      }
      return retry.onHeap.get(kademliaId);
    } catch (Exception e) {
      logger.warn(
          "could not read node {} from the node cache, dropping the on-disk tier", kademliaId, e);
      Log.sentry(e);
      if (live.onDisk != null) {
        live.onDisk.clear();
      }
      return null;
    }
  }

  public void remove(KademliaId kademliaId) {
    NodeStore live = liveStore();
    if (live == null) {
      reportNoLiveStore();
      return;
    }
    try {
      live.onHeap.remove(kademliaId);
    } catch (IllegalAccessError closedUnderUs) {
      NodeStore retry = live.liveStore();
      if (retry == null) {
        reportNoLiveStore();
        return;
      }
      retry.onHeap.remove(kademliaId);
    }
  }

  public void saveToDisk() {
    synchronized (lifecycleLock) {
      if (closed) {
        // Shutdown already closed the tiers under us; there is nothing left to flush, and MapDB's
        // "Store was closed" is not a corrupt cache.
        return;
      }
      saveToDiskLocked();
    }
  }

  private void saveToDiskLocked() {

    if (offHeap == null) {
      // Memory-only store (buildWithMemoryCacheOnly): there is nothing to flush, and running the
      // recovery below on it would be a self-inflicted wipe.
      return;
    }

    try {
      offHeap.clearWithExpire();
      onHeap.clearWithExpire();
      Runnable seam = betweenFlushStepsForTest;
      if (seam != null) {
        seam.run();
      }
      offHeap.clearWithExpire();
    } catch (Throwable e) {
      logger.warn("NodeStore may be broken, closing and reopening the store", e);
      Log.sentry(e);

      close();
      Path path = Path.of(nodeCachePath(serverContext.getPort()));
      try {
        Files.delete(path);
      } catch (IOException ex) {
        logger.warn("could not delete the broken node cache {}", path, ex);
      }
      // TD185: this used to install `new NodeStore(serverContext)` — the bare private constructor,
      // which leaves onHeap/offHeap/onDisk null and replaces the live node graph with an empty
      // one. Recovery therefore produced a store whose every get() threw
      // `NullPointerException: ... because "this.onHeap" is null`, which
      // ConnectionHandler.setupConnection turns into "Handshake failed with throwable": after one
      // failed save the node dropped EVERY new inbound connection, and the DHT jobs spun on
      // `IllegalArgumentException: no such vertex in graph` against the discarded graph. A store
      // that cannot be read is not a recovery. Rebuild through the real builder instead, which
      // re-adopts the persisted graph and re-creates all three tiers.
      try {
        serverContext.setNodeStore(buildWithDiskCache(serverContext, this));
      } catch (RuntimeException | Error rebuildFailure) {
        // The file-backed rebuild is the only step here that can fail on its own (the file could
        // not be deleted, the mmap could not be taken). A node without a disk cache still routes;
        // a node without a store at all does not.
        logger.error(
            "could not rebuild the on-disk node cache, continuing without one", rebuildFailure);
        Log.sentry(rebuildFailure);
        serverContext.setNodeStore(buildWithMemoryCacheOnly(serverContext, this));
      }
    }
  }

  /**
   * Closes all cache tiers and this store's expiry executor.
   *
   * <p>Null-safe per tier: {@link #buildWithMemoryCacheOnly(ServerContext)} builds a store with
   * only the on-heap tier.
   */
  public void close() {
    synchronized (lifecycleLock) {
      if (closed) {
        // Server.shutdown() runs twice on a job-triggered restart (ServerRestartJob, then the JVM
        // shutdown hook); the second close must not touch the tiers again.
        return;
      }
      closed = true;

      closeQuietly(onHeap);
      closeQuietly(offHeap);
      closeQuietly(onDisk);

      closeQuietly(dbonHeap);
      closeQuietly(dboffHeap);
      closeQuietly(dbDisk);

      threadPool.shutdownNow();
    }
  }

  /**
   * Plain volatile read: must not wait for an in-flight {@link #saveToDisk()}, see {@link
   * #liveStore()}.
   */
  boolean isClosed() {
    return closed;
  }

  /**
   * Test hook: kills the on-disk tier underneath a live store, which is what a corrupt cache file
   * looks like to {@link #saveToDisk()} — the flush throws out of {@code clearWithExpire()} while
   * the store itself is still open. Unlike {@link #close()} this must drive the recovery path.
   */
  void breakDiskTierForTest() {
    synchronized (lifecycleLock) {
      if (closed || dbDisk == null) {
        throw new IllegalStateException(
            "breakDiskTierForTest needs an open store with an on-disk tier");
      }
      dbDisk.close();
    }
  }

  private static void closeQuietly(java.io.Closeable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (IOException | RuntimeException e) {
      logger.warn("could not close a node cache tier", e);
    }
  }

  /**
   * Flushes the cache and returns the number of entries in the lowest tier the store has — the
   * on-disk tier, or the on-heap one for a memory-only store.
   *
   * <p>TD212: this used to read {@code this.onDisk} after the {@code saveToDisk()} above, which is
   * exactly the call that can replace this store. On the recovery path {@code this} is the closed
   * predecessor by the time the read happens, so the size read threw instead of answering. It now
   * reads the tier of the store that is live afterwards (see {@link #liveStore()}); after a
   * recovery that is a freshly built one, so the honest answer is the size of the rebuilt cache,
   * not of the cache that was just thrown away.
   */
  public int size() {
    saveToDisk();
    NodeStore live = liveStore();
    if (live == null) {
      reportNoLiveStore();
      return 0;
    }
    if (live != this) {
      // The flush above ran on this store and then replaced it, so the successor's own in-memory
      // tiers have not been flushed yet — honour the "flush, then count" contract for it as well
      // (T149 review finding). If that flush recovers too, re-resolve once: two hops, no recursion.
      live.saveToDisk();
      live = live.liveStore();
      if (live == null) {
        reportNoLiveStore();
        return 0;
      }
    }
    if (live.onDisk == null) {
      return live.onHeap.size();
    }
    return live.onDisk.size();
  }

  public void maintainNodes() {

    // all graph mutations have to run under the write lock, otherwise jobs
    // holding the lock (e.g. PeerPerformanceTestGarlicMessageJob) can observe
    // edges vanishing mid-iteration (Sentry REDPANDAJ-2DW)
    readWriteLock.writeLock().lock();
    try {
      decayRandomEdge();

      addServerEdges();

      removeNodeIfNoGoodLinkAvailable();

      removeBadScoredNode();

      addRandomNodeToGraph();

      if (nodeGraph.edgeSet().size() < MAX_EDGES_IN_GRAPH) {
        addRandomEdgeIfWaitedEnough();
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  private void removeBadScoredNode() {
    ArrayList<Node> nodes = new ArrayList<>(nodeGraph.vertexSet());
    if (nodes.size() < 4) {
      return;
    }
    for (Node node : nodes) {
      if (node.equals(serverContext.getNode())) {
        continue;
      }

      if (!node.getNodeId().hasKey()) {
        // this may be an old own server id...
        remove(node.getNodeId().getKademliaId());
      }

      if (node.getScore() < -50) {

        int veryGoodLinks = 0;
        for (NodeEdge nodeEdge : nodeGraph.outgoingEdgesOf(node)) {
          if (nodeGraph.getEdgeWeight(nodeEdge) < 5) {
            veryGoodLinks++;
          }
        }
        if (veryGoodLinks <= 1) {
          logger.debug(
              "remove node {} due to bad score of {}, very good links only {}",
              node,
              node.getScore(),
              veryGoodLinks);
          removeNodeFromGraphAndBlacklist(node);
        }
      }
    }
  }

  private void decayRandomEdge() {
    ArrayList<NodeEdge> nodeEdges = new ArrayList<>(nodeGraph.edgeSet());
    if (nodeEdges.size() < 10) {
      return;
    }
    NodeEdge randomEdge = nodeEdges.get(random.nextInt(nodeEdges.size()));
    double edgeWeight = nodeGraph.getEdgeWeight(randomEdge);
    edgeWeight++;
    if (edgeWeight > PeerPerformanceTestGarlicMessageJob.MAX_WEIGHT) {
      edgeWeight = PeerPerformanceTestGarlicMessageJob.MAX_WEIGHT;
    }
    nodeGraph.setEdgeWeight(randomEdge, edgeWeight);
  }

  private void addServerEdges() {
    Node serverNode = serverContext.getNode();
    if (nodeGraph.outgoingEdgesOf(serverNode).size() < 15
        || nodeGraph.incomingEdgesOf(serverNode).size() < 15) {

      // snapshot under the peer list read lock, the live list is modified by
      // network threads (Sentry REDPANDAJ-2DZ)
      List<Peer> peersSnapshot = serverContext.getPeerList().snapshot();

      for (Peer peer : peersSnapshot) {
        if (!peer.isConnected()
            || peer.getNode() == null
            || !nodeGraph.containsVertex(peer.getNode())) {
          continue;
        }
        nodeGraph.addEdge(serverNode, peer.getNode());
        nodeGraph.addEdge(peer.getNode(), serverNode);
      }
    }
  }

  private void addRandomNodeToGraph() {
    int currentNodeCount = nodeGraph.vertexSet().size();

    if (currentNodeCount < MAX_NODES_FOR_GRAPH) {
      // Through liveStore() like the accessors above: maintainNodes() does not run under
      // lifecycleLock, so a recovery can close this store mid-tick and onHeap.entrySet() would
      // throw under the graph write lock (T149 review finding).
      NodeStore live = liveStore();
      if (live == null) {
        return;
      }

      ArrayList<Map.Entry<KademliaId, Node>> entries = new ArrayList<>(live.onHeap.entrySet());

      Collections.sort(entries, Comparator.comparingInt(a -> -a.getValue().getScore()));

      for (Map.Entry<KademliaId, Node> o : entries) {
        Node nodeToAdd = o.getValue();

        if (nodeToAdd.isBlacklisted()) {
          continue;
        }

        if (!nodeGraph.containsVertex(nodeToAdd)) {
          addNodeWithInitialEdges(nodeToAdd);
          break;
        }
      }
    }
  }

  private void addNodeWithInitialEdges(Node nodeToAdd) {
    nodeGraph.addVertex(nodeToAdd);
    Node randomEdge = getRandomNode(nodeToAdd);
    if (randomEdge != null) {
      NodeEdge defaultEdge = nodeGraph.addEdge(nodeToAdd, randomEdge);
      nodeGraph.setEdgeWeight(defaultEdge, PeerPerformanceTestGarlicMessageJob.CUT_HARD);
    }
  }

  private void removeNodeIfNoGoodLinkAvailable() {

    List<Node> nodes = new ArrayList<>(nodeGraph.vertexSet());
    nodes.remove(serverContext.getNode());
    if (nodes.size() < 4) {
      return;
    }

    Node nodeToRemove = null;
    for (Node node : nodes) {

      boolean oneGoodLink = isOneGoodLinkAvailable(node);

      if (!oneGoodLink && nodeGraph.edgesOf(node).size() >= MIN_EDGES_NEEDED_FOR_NODE_REMOVAL) {
        nodeToRemove = node;
        break;
      }
    }
    if (nodeToRemove != null) {
      removeNodeFromGraphAndBlacklist(nodeToRemove);
    }
  }

  private void removeNodeFromGraphAndBlacklist(Node nodeToRemove) {
    nodeToRemove.touchBlacklisted();
    nodeGraph.removeVertex(nodeToRemove);
    logger.debug("removed node since no good link available: {}", nodeToRemove);
  }

  private boolean isOneGoodLinkAvailable(Node node) {
    for (NodeEdge edge : nodeGraph.edgesOf(node)) {
      if (nodeGraph.getEdgeWeight(edge) <= PeerPerformanceTestGarlicMessageJob.LINK_FAILED) {
        return true;
      }
    }
    return false;
  }

  private void addRandomEdgeIfWaitedEnough() {
    boolean allEdgesGood = true;
    for (NodeEdge edge : nodeGraph.edgeSet()) {
      if (nodeGraph.getEdgeWeight(edge) < 5) {
        allEdgesGood = false;
        break;
      }
    }

    if (allEdgesGood || System.currentTimeMillis() - lastTimeEdgeAdded > 1000L * 10L) {
      addRandomEdge();
      lastTimeEdgeAdded = System.currentTimeMillis();
    }
  }

  private void addRandomEdge() {
    Set<Node> nodes = nodeGraph.vertexSet();

    if (nodes.size() < 2) {
      return;
    }

    ArrayList<Node> ids = new ArrayList<>(nodes);

    boolean added = false;
    int count = 0;

    while (!added && count < 10) {
      count++;

      Collections.shuffle(ids);
      Node nodeFrome = ids.getFirst();
      ids.remove(nodeFrome);
      Collections.shuffle(ids);
      Node nodeTo = ids.getFirst();
      ids.add(nodeFrome);
      ids.add(nodeTo);

      if (nodeFrome.equals(nodeTo)) {
        continue;
      }

      NodeEdge defaultEdge = nodeGraph.addEdge(nodeFrome, nodeTo);

      if (defaultEdge != null) {
        nodeGraph.setEdgeWeight(defaultEdge, PeerPerformanceTestGarlicMessageJob.CUT_HARD);
        added = true;
        logger.debug("added edge: {} -> {}", nodeFrome.getNodeId(), nodeTo.getNodeId());
      }
    }
  }

  private Node getRandomNode(Node exclude) {
    ArrayList<Node> nodes = new ArrayList<>(nodeGraph.vertexSet());
    nodes.remove(exclude);
    Collections.shuffle(nodes);
    if (nodes.isEmpty()) {
      return null;
    }
    return nodes.getFirst();
  }

  public DefaultDirectedWeightedGraph<Node, NodeEdge> getNodeGraph() {
    return nodeGraph;
  }

  public void printBlacklist() {
    NodeStore live = liveStore();
    if (live == null) {
      return;
    }
    for (Object value : live.onHeap.getValues()) {
      Node node = (Node) value;
      if (node.isBlacklisted()) {
        System.out.println(node);
      }
    }
  }

  public void clearGraph() {
    nodeGraph = new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    nodeGraph.addVertex(serverContext.getNode());
  }

  /** The node cache file of {@code port} (T117: explicit values, hence the new name). */
  static String nodeCachePath(int port) {
    return "data/nodecache" + port + ".mapdb";
  }

  /**
   * The pre-T117 node cache held Elsa-serialized {@code Node} objects, i.e. fully qualified class
   * names. It is not read and not migrated — the cache is rebuilt from the network (user decision
   * 2026-09-01). Reported once per start so the file can be deleted by hand.
   */
  private static void logStaleLegacyCache(int port) {
    Path legacy = Path.of("data/nodeids" + port + ".mapdb");
    if (Files.exists(legacy)) {
      Log.put(
          "ignoring the pre-T117 node cache "
              + legacy
              + ": it is no longer read and can be deleted",
          20);
    }
  }

  public void clearNodeBlacklist() {
    NodeStore live = liveStore();
    if (live == null) {
      return;
    }
    for (Object value : live.onHeap.getValues()) {
      Node node = (Node) value;
      node.resetBlacklisted();
      node.setGmTestsSuccessful(0);
      node.setGmTestsFailed(0);
    }
  }
}
