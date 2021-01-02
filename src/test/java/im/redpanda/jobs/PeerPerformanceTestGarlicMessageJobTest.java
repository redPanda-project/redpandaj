package im.redpanda.jobs;

import im.redpanda.core.LocalSettings;
import im.redpanda.core.Node;
import im.redpanda.core.NodeId;
import im.redpanda.core.Server;
import im.redpanda.flaschenpost.GMContent;
import im.redpanda.flaschenpost.GMParser;
import im.redpanda.store.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.Security;
import java.util.ArrayList;

public class PeerPerformanceTestGarlicMessageJobTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

    }


    @Test
    public void calculateNestedGarlicMessagesTest() {
        ArrayList<Node> nodes = new ArrayList<Node>();

        Node nodeA = new Node(Server.nodeId);
        nodes.add(nodeA);

        Node nodeB = new Node(Server.nodeId);
        nodes.add(nodeB);

        byte[] bytes = PeerPerformanceTestGarlicMessageJob.calculateNestedGarlicMessages(nodes, 1);

        GMContent parse = GMParser.parse(bytes);
    }

    @Before
    public void setUp() throws Exception {
        Server.MY_PORT = 1;
        Server.localSettings = new LocalSettings();
        Server.nodeStore = new NodeStore();
        Server.nodeId = new NodeId();
    }

    @After
    public void tearDown() throws Exception {
        Server.nodeStore.close();
    }
}