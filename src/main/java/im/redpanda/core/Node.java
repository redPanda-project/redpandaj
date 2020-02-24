package im.redpanda.core;

import im.redpanda.store.NodeStore;

import java.io.Serializable;

public class Node implements Serializable {

    private NodeId nodeId;
    private long lastSeen;

    /**
     * Creates a new Node and adds the Node to the NodeStore.
     *
     * @param nodeId
     */
    public Node(NodeId nodeId) {
        this.nodeId = nodeId;
        lastSeen = System.currentTimeMillis();
        Server.nodeStore.put(nodeId.getKademliaId(), this);
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

    public static Node getByKademliaId(KademliaId id) {
        return Server.nodeStore.get(id);
    }


}
