package im.redpanda.routing.graph;

import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import im.redpanda.crypt.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A node of the DHT/graph as this node knows it. Persisted as explicit JSON (see {@code NodeCodec})
 * — not {@code Serializable} since T117, so moving this class cannot make the node graph or the
 * node cache unreadable.
 */
public class Node {

  static final int MAX_SCORE_VALUE = 50;

  /** A connection point not seen for two weeks is dropped from the persisted node. */
  static final long CONNECTION_POINT_MAX_AGE_MS = 1000L * 60L * 60L * 24L * 14L;

  private static final Logger logger = LogManager.getLogger();

  private final NodeId nodeId;
  private long lastSeen;
  private ArrayList<ConnectionPoint> connectionPoints;
  private int gmTestsSuccessful = 0;
  private int gmTestsFailed = 0;
  private long blacklistedSince = 0;

  /**
   * Creates a new Node and adds the Node to the NodeStore.
   *
   * @param nodeId
   */
  public Node(ServerContext serverContext, NodeId nodeId) {
    this.nodeId = nodeId;
    lastSeen = System.currentTimeMillis();
    serverContext.getNodeStore().put(nodeId.getKademliaId(), this);
    // run the get command afterwards to trigger the eviction timer
    serverContext.getNodeStore().get(nodeId.getKademliaId());
    connectionPoints = new ArrayList<>();
  }

  /**
   * Restores a persisted node (T117). Unlike {@code Node(ServerContext, NodeId)} this does
   * <b>not</b> touch a NodeStore: the caller is the storage layer, which is either filling the
   * graph of {@code LocalSettings} or the node cache itself.
   */
  Node(
      NodeId nodeId,
      long lastSeen,
      ArrayList<ConnectionPoint> connectionPoints,
      int gmTestsSuccessful,
      int gmTestsFailed,
      long blacklistedSince) {
    this.nodeId = nodeId;
    this.lastSeen = lastSeen;
    this.connectionPoints = connectionPoints;
    this.gmTestsSuccessful = gmTestsSuccessful;
    this.gmTestsFailed = gmTestsFailed;
    this.blacklistedSince = blacklistedSince;
  }

  public static void addNodeIfNotPresent(
      ServerContext serverContext, NodeId nodeId, String ip, int port) {
    Node byKademliaId = getByKademliaId(serverContext, nodeId.getKademliaId());
    if (byKademliaId == null) {
      Node node = new Node(serverContext, nodeId);
      if (ip != null) {
        node.addConnectionPoint(ip, port);
      }
    }
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public long getLastSeen() {
    return lastSeen;
  }

  public void seen() {
    this.lastSeen = System.currentTimeMillis();
  }

  /** The connection points currently known for this node (live list, not a copy). */
  List<ConnectionPoint> getConnectionPoints() {
    return connectionPoints;
  }

  public ConnectionPoint latestSeenConnectionPoint() {
    if (connectionPoints.isEmpty()) {
      return null;
    }
    Collections.sort(connectionPoints, Comparator.comparingLong(ConnectionPoint::getLastSeen));
    return connectionPoints.getFirst();
  }

  /**
   * Records that this node was seen at {@code ip:port} and evicts every connection point that has
   * not been seen for {@link #CONNECTION_POINT_MAX_AGE_MS}.
   *
   * <p>The loop used to {@code break} on the first match, so the staleness scan only ever covered
   * the entries before the match — and never ran at all for a long-lived stable connection, whose
   * point sits at the front of the list and is re-confirmed by {@code NodeConnectionPointsSeenJob}
   * every two minutes. The entries are persisted, so a node seen from many addresses over time
   * (mobile/NAT churn, replayed handshakes from many source IPs) accumulated connection points
   * without the intended 14-day bound.
   */
  public void seen(String ip, int port) {
    seen();
    ConnectionPoint connectionPoint = new ConnectionPoint(ip, port);

    ArrayList<ConnectionPoint> toRemove = new ArrayList<>();
    long staleBefore = this.lastSeen - CONNECTION_POINT_MAX_AGE_MS;

    boolean found = false;
    for (ConnectionPoint point : connectionPoints) {
      if (!found && point.equals(connectionPoint)) {
        point.setLastSeen(this.lastSeen);
        point.resetRetries();
        found = true;
        // just refreshed, so it can never be stale — and the scan must continue over the rest
        continue;
      }

      if (point.getLastSeen() < staleBefore) {
        logger.info("removed connection point since not seen since two weeks: " + point.getIp());
        toRemove.add(point);
      }
    }

    connectionPoints.removeAll(toRemove);

    if (!found) {
      connectionPoints.add(connectionPoint);
    }

    for (ConnectionPoint point : connectionPoints) {
      logger.debug(
          "seen, ip list for node: %s:%s reties: %s, last seen: %s"
              .formatted(
                  point.getIp(),
                  point.getPort(),
                  point.getRetries(),
                  Utils.formatDurationFromNow(point.getLastSeen())));
    }
  }

  public static Node getByKademliaId(ServerContext serverContext, KademliaId id) {
    if (id == null) {
      return null;
    }
    return serverContext.getNodeStore().get(id);
  }

  public boolean addConnectionPoint(String ip, int port) {
    ConnectionPoint connectionPoint = new ConnectionPoint(ip, port);

    // todo can be removed later
    if (connectionPoints == null) {
      connectionPoints = new ArrayList<>();
    }

    if (connectionPoints.contains(connectionPoint)) {
      return false;
    }
    connectionPoints.add(connectionPoint);
    return true;
  }

  public int incrRetry(String ip, int port) {
    ConnectionPoint connectionPoint = new ConnectionPoint(ip, port);
    for (ConnectionPoint point : connectionPoints) {
      if (point.equals(connectionPoint)) {
        point.IncrRetries();
        return point.getRetries();
      }
    }

    //        for (ConnectionPoint point : connectionPoints) {
    //            System.out.println("ip list for node: " + point.getIp() + ":" + point.getPort() +
    // " reties: " + point.getRetries());
    //        }
    return 0;
  }

  /**
   * The blacklist timestamp, for the storage layer only. A getter rather than a widened field, so
   * persistence does not enlarge the mutation surface of Node.
   */
  long blacklistedSince() {
    return blacklistedSince;
  }

  public void resetBlacklisted() {
    blacklistedSince = 0;
  }

  public static class ConnectionPoint {
    String ip;
    int port;
    long lastSeen;
    int retries;

    public ConnectionPoint(String ip, int port) {
      this(ip, port, System.currentTimeMillis(), 0);
    }

    /** Restores a persisted connection point (T117); see the restoring Node constructor. */
    ConnectionPoint(String ip, int port, long lastSeen, int retries) {
      this.ip = ip;
      this.port = port;
      this.lastSeen = lastSeen;
      this.retries = retries;
    }

    public long getLastSeen() {
      return lastSeen;
    }

    public int getPort() {
      return port;
    }

    public String getIp() {
      return ip;
    }

    public int getRetries() {
      return retries;
    }

    public void setLastSeen(long lastSeen) {
      this.lastSeen = lastSeen;
    }

    public void IncrRetries() {
      retries++;
    }

    public void resetRetries() {
      retries = 0;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ConnectionPoint that = (ConnectionPoint) o;

      if (port != that.port) {
        return false;
      }
      return ip != null ? ip.equals(that.ip) : that.ip == null;
    }

    @Override
    public int hashCode() {
      int result = ip != null ? ip.hashCode() : 0;
      result = 31 * result + port;
      return result;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Node node = (Node) o;

    return nodeId.equals(node.nodeId);
  }

  @Override
  public int hashCode() {
    return nodeId.hashCode();
  }

  public int getGmTestsSuccessful() {
    return gmTestsSuccessful;
  }

  public void setGmTestsSuccessful(int gmTestsSuccessful) {
    this.gmTestsSuccessful = gmTestsSuccessful;
  }

  public int getGmTestsFailed() {
    return gmTestsFailed;
  }

  public void setGmTestsFailed(int gmTestsFailed) {
    this.gmTestsFailed = gmTestsFailed;
  }

  public void increaseGmTestsFailed() {
    this.gmTestsFailed++;
  }

  public void increaseGmTestsSuccessful() {
    this.gmTestsSuccessful++;
  }

  public int getScore() {
    int score =
        Math.min(MAX_SCORE_VALUE, gmTestsSuccessful) * 3
            - Math.min(MAX_SCORE_VALUE, gmTestsFailed) * 5;

    if (System.currentTimeMillis() - lastSeen > 1000L * 60L * 60L) {
      score -= 20;
    }

    return score;
  }

  public void cleanChecks() {
    // todo: make threadsafe?
    if (getGmTestsFailed() > MAX_SCORE_VALUE) {
      setGmTestsFailed(MAX_SCORE_VALUE);
    }

    if (getGmTestsSuccessful() > MAX_SCORE_VALUE) {
      setGmTestsSuccessful(MAX_SCORE_VALUE);
      if (getGmTestsFailed() > 0) {
        gmTestsFailed--;
        setGmTestsSuccessful(MAX_SCORE_VALUE - 5);
      }
    }
  }

  @Override
  public String toString() {
    return nodeId.getKademliaId().toString();
  }

  public boolean isBlacklisted() {
    return blacklistedSince != 0
        && System.currentTimeMillis() - blacklistedSince < NodeStore.NODE_BLACKLISTED_FOR_GRAPH;
  }

  public void touchBlacklisted() {
    blacklistedSince = System.currentTimeMillis();
    setGmTestsSuccessful(0);
    setGmTestsFailed(0);
  }
}
