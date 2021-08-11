package im.redpanda.core;

import im.redpanda.store.NodeStore;
import org.junit.Test;

import java.security.Security;

import static org.junit.Assert.assertTrue;

public class NodeTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void getByKademliaId() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeStore nodeStore = serverContext.getNodeStore();

        int size = nodeStore.size();
        System.out.println("Size of NodeStore: " + size);

        Node node = new Node(serverContext, new NodeId());

        KademliaId kademliaId = node.getNodeId().getKademliaId();

        Node byKademliaId = Node.getByKademliaId(serverContext, kademliaId);

        assertTrue(byKademliaId != null);

        assertTrue(byKademliaId.getNodeId().getKademliaId().equals(kademliaId));

        assertTrue(byKademliaId.getNodeId().hasPrivate());

        assertTrue(nodeStore.size() - size == 1);

    }
}