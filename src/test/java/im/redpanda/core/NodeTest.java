package im.redpanda.core;

import im.redpanda.store.NodeStore;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class NodeTest {


    static {
        Server.localSettings = new LocalSettings();
    }

    @Test
    public void getByKademliaId() {


        new File("data").mkdirs();

        NodeStore nodeStore = Server.nodeStore;
        if (nodeStore == null) {
            nodeStore = new NodeStore();
            Server.nodeStore = nodeStore;
        }

        int size = nodeStore.size();
        System.out.println("Size of NodeStore: " + size);

        Node node = new Node(new NodeId());

        KademliaId kademliaId = node.getNodeId().getKademliaId();

        Node byKademliaId = Node.getByKademliaId(kademliaId);

        assertTrue(byKademliaId != null);

        assertTrue(byKademliaId.getNodeId().getKademliaId().equals(kademliaId));

        assertTrue(byKademliaId.getNodeId().hasPrivate());

//        byKademliaId.seen("test", -1);

        assertTrue(nodeStore.size() - size == 1);

//        nodeStore.close();


    }
}