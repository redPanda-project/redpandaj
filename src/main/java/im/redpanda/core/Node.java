package im.redpanda.core;

import im.redpanda.crypt.Utils;
import im.redpanda.store.NodeStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;

public class Node implements Serializable {

    static final int MAX_SCORE_VALUE = 15;

    private static final Logger logger = LogManager.getLogger();

    private static final long serialVersionUID = 43L;
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
        //run the get command afterwards to trigger the eviction timer
        serverContext.getNodeStore().get(nodeId.getKademliaId());
        connectionPoints = new ArrayList<>();
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

    //ToDo run seen every once in a while to trigger seen for peers which are permanently connected
    public void seen(String ip, int port) {
        seen();
        ConnectionPoint connectionPoint = new ConnectionPoint(ip, port);

        ArrayList<ConnectionPoint> toRemove = new ArrayList<>();

        boolean found = false;
        for (ConnectionPoint point : connectionPoints) {
            if (point.equals(connectionPoint)) {
                point.setLastSeen(this.lastSeen);
                point.resetRetries();
                found = true;
                break;
            }

            if (point.lastSeen < this.lastSeen - 1000L * 60L * 60L * 24L * 14L) {
                logger.info("removed connection point since not seen since two weeks: " + point.getIp());
                toRemove.add(point);
            }
        }

        connectionPoints.removeAll(toRemove);

        if (!found) {
            connectionPoints.add(connectionPoint);
        }

        for (ConnectionPoint point : connectionPoints) {
            logger.debug(String.format("seen, ip list for node: %s:%s reties: %s, last seen: %s", point.getIp(), point.getPort(), point.getRetries(), Utils.formatDurationFromNow(point.getLastSeen())));
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

        //todo can be removed later
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
//            System.out.println("ip list for node: " + point.getIp() + ":" + point.getPort() + " reties: " + point.getRetries());
//        }
        return 0;
    }

    public void resetBlacklisted() {
        blacklistedSince = 0;
    }

    public static class ConnectionPoint implements Serializable {
        String ip;
        int port;
        long lastSeen;
        int retries;

        public ConnectionPoint(String ip, int port) {
            this.ip = ip;
            this.port = port;
            lastSeen = System.currentTimeMillis();
            retries = 0;
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
        int score = Math.min(MAX_SCORE_VALUE, gmTestsSuccessful) * 3 - Math.min(MAX_SCORE_VALUE, gmTestsFailed) * 5;

        if (System.currentTimeMillis() - lastSeen > 1000L * 60L * 60L) {
            score -= 50;
        }

        return score;
    }

    public void cleanChecks() {
        //todo: make threadsafe?
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
        return blacklistedSince != 0 && System.currentTimeMillis() - blacklistedSince < NodeStore.NODE_BLACKLISTED_FOR_GRAPH;
    }

    public void touchBlacklisted() {
        blacklistedSince = System.currentTimeMillis();
        setGmTestsSuccessful(0);
        setGmTestsFailed(0);
    }
}


