package im.redpanda.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;

public class Node implements Serializable {

    private static final Logger logger = LogManager.getLogger();

    private static final long serialVersionUID = 43L;
    private final NodeId nodeId;
    private long lastSeen;
    private ArrayList<ConnectionPoint> connectionPoints;
    private int gmTestsSuccessful = 0;
    private int gmTestsFailed = 0;

    /**
     * Creates a new Node and adds the Node to the NodeStore.
     *
     * @param nodeId
     */
    public Node(NodeId nodeId) {
        this.nodeId = nodeId;
        lastSeen = System.currentTimeMillis();
        Server.nodeStore.put(nodeId.getKademliaId(), this);
        //run the get command afterwards to trigger the eviction timer
        Server.nodeStore.get(nodeId.getKademliaId());
        connectionPoints = new ArrayList<>();
    }


    public NodeId getNodeId() {
        return nodeId;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    //ToDo run seen every once in a while to trigger seen for peers which are permanently connected
    public void seen(String ip, int port) {
        this.lastSeen = System.currentTimeMillis();
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
            logger.debug("seen, ip list for node: " + point.getIp() + ":" + point.getPort() + " reties: " + point.getRetries());
        }

    }

    public static Node getByKademliaId(KademliaId id) {
        if (id == null) {
            return null;
        }
        return Server.nodeStore.get(id);
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
}


