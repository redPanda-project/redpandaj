package im.redpanda.core;

import im.redpanda.store.NodeStore;
import org.junit.Test;

import static org.junit.Assert.*;

public class NodeTest {


    @Test
    public void getByKademliaId() {


        NodeStore nodeStore = new NodeStore();
        Server.nodeStore = nodeStore;

        int size = nodeStore.size();
        System.out.println("Size of NodeStore: " + size);

        Node node = new Node(new NodeId());

        KademliaId kademliaId = node.getNodeId().getKademliaId();

        Node byKademliaId = Node.getByKademliaId(kademliaId);

        assertTrue(byKademliaId != null);

        assertTrue(byKademliaId.getNodeId().getKademliaId().equals(kademliaId));

        assertTrue(byKademliaId.getNodeId().hasPrivate());

        byKademliaId.seen();

        assertTrue(nodeStore.size() - size == 1);

        nodeStore.close();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}