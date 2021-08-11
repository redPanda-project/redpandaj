package im.redpanda.jobs;

import im.redpanda.core.*;
import im.redpanda.flaschenpost.GMContent;
import im.redpanda.flaschenpost.GMParser;
import im.redpanda.store.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.Security;
import java.util.ArrayList;

public class PeerPerformanceTestGarlicMessageJobTest {

    private final static ServerContext serverContext = new ServerContext();

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

        PeerPerformanceTestGarlicMessageJob peerPerformanceTestGarlicMessageJob = new PeerPerformanceTestGarlicMessageJob(serverContext);

        byte[] bytes = peerPerformanceTestGarlicMessageJob.calculateNestedGarlicMessages(nodes, 1);

        GMContent parse = GMParser.parse(serverContext, bytes);
        //todo assert?
    }

    @Before
    public void setUp() throws Exception {
        Server.localSettings = new LocalSettings();
        Server.nodeStore = new NodeStore(serverContext);
        Server.nodeId = new NodeId();
    }

    @After
    public void tearDown() throws Exception {
        Server.nodeStore.close();
    }
}