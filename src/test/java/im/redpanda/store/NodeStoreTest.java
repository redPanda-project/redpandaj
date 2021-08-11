package im.redpanda.store;

import im.redpanda.core.Node;
import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import org.junit.Test;

import java.security.Security;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class NodeStoreTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void testBlacklist() {
        Map<Node, Long> nodeBlacklist = new HashMap<>();

        Node node = new Node(ServerContext.buildDefaultServerContext(), new NodeId());

        nodeBlacklist.put(node, System.currentTimeMillis());

        assertTrue(nodeBlacklist.containsKey(node));

        assertTrue(System.currentTimeMillis() - nodeBlacklist.get(node) < 1000L * 60L * 5L);

    }

}