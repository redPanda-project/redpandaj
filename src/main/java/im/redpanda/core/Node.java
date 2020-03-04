package im.redpanda.core;

import java.io.Serializable;
import java.util.ArrayList;

public class Node implements Serializable {

    private static final long serialVersionUID = 42L;
    private NodeId nodeId;
    private long lastSeen;
    private ArrayList<ConnectionPoint> connectionPoints;

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

    //ToDo run seen every once in a while to trigger seen for peers which are permanetly connected
    public void seen(String ip, int port) {
        this.lastSeen = System.currentTimeMillis();
        ConnectionPoint connectionPoint = new ConnectionPoint(ip, port);
        boolean found = false;
        for (ConnectionPoint point : connectionPoints) {
            if (point.equals(connectionPoint)) {
                point.setLastSeen(this.lastSeen);
                point.resetRetries();
                found = true;
                break;
            }
        }

        if (!found) {
            connectionPoints.add(connectionPoint);
        }

        for (ConnectionPoint point : connectionPoints) {
            System.out.println("seen, ip list for node: " + point.getIp() + ":" + point.getPort() + " reties: " + point.getRetries());
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

    public void incrRetry(String ip, int port) {
        ConnectionPoint connectionPoint = new ConnectionPoint(ip, port);
        for (ConnectionPoint point : connectionPoints) {
            if (point.equals(connectionPoint)) {
                point.IncrRetries();
                break;
            }
        }

        for (ConnectionPoint point : connectionPoints) {
            System.out.println("ip list for node: " + point.getIp() + ":" + point.getPort() + " reties: " + point.getRetries());
        }

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

}


