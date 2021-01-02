package im.redpanda.store;

import im.redpanda.core.*;
import org.junit.Test;

import java.security.Security;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class NodeStoreTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void testBlacklist() {

        Server.localSettings = new LocalSettings();
        Server.nodeStore = new NodeStore();

        Map<Node, Long> nodeBlacklist = new HashMap<>();

        Node node = new Node(new NodeId());

        nodeBlacklist.put(node, System.currentTimeMillis());

        assertTrue(nodeBlacklist.containsKey(node));

        assertTrue(System.currentTimeMillis() - nodeBlacklist.get(node) < 1000L * 60L * 5L);

    }

}