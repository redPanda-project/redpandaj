package im.redpanda.flaschenpost;

import im.redpanda.core.Command;
import im.redpanda.core.Node;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.core.ServerContext;
import im.redpanda.jobs.Job;
import im.redpanda.jobs.PeerPerformanceTestFlaschenpostJob;
import im.redpanda.jobs.PeerPerformanceTestGarlicMessageJob;
import im.redpanda.kademlia.KadContent;
import im.redpanda.kademlia.nodeinfo.GMEntryPointModel;
import im.redpanda.kademlia.nodeinfo.NodeInfoModel;
import im.redpanda.store.NodeEdge;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GMParserAdditionalTest {

    private static final int BUFFER_PADDING = 32;

    private static byte[] garlicMessageBytes(ServerContext serverContext, NodeId target) {
        GarlicMessage garlicMessage = new GarlicMessage(serverContext, target);
        return garlicMessage.getContent();
    }

    private static void registerJob(Job job, int jobId) {
        try {
            Field jobIdField = Job.class.getDeclaredField("jobId");
            jobIdField.setAccessible(true);
            jobIdField.setInt(job, jobId);

            Field runningJobsField = Job.class.getDeclaredField("runningJobs");
            runningJobsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            HashMap<Integer, Job> runningJobs = (HashMap<Integer, Job>) runningJobsField.get(null);

            Field lockField = Job.class.getDeclaredField("runningJobsLock");
            lockField.setAccessible(true);
            ReentrantLock lock = (ReentrantLock) lockField.get(null);

            lock.lock();
            try {
                runningJobs.put(jobId, job);
            } finally {
                lock.unlock();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class TestPeer extends Peer {
        boolean setWriteBufferCalled;

        TestPeer(String ip, int port) {
            super(ip, port);
        }

        TestPeer(String ip, int port, NodeId nodeId) {
            super(ip, port, nodeId);
        }

        @Override
        public boolean setWriteBufferFilled() {
            setWriteBufferCalled = true;
            return true;
        }
    }

    private static class StubFlaschenpostJob extends PeerPerformanceTestFlaschenpostJob {
        boolean successCalled;

        StubFlaschenpostJob(ServerContext serverContext) {
            super(serverContext, new TestPeer("127.0.0.1", 9999));
        }

        @Override
        public void success() {
            successCalled = true;
        }
    }

    private static class StubGarlicJob extends PeerPerformanceTestGarlicMessageJob {
        boolean successCalled;

        StubGarlicJob(ServerContext serverContext) {
            super(serverContext);
        }

        @Override
        public void success() {
            successCalled = true;
        }
    }

    private static class NullPeerList extends PeerList {
        NullPeerList(ServerContext serverContext) {
            super(serverContext);
        }

        @Override
        public ArrayList<Peer> getPeerArrayList() {
            return null;
        }
    }

    @Test(expected = RuntimeException.class)
    public void parseUnknownTypeThrows() {
        byte[] content = new byte[]{(byte) 99, 0, 0, 0};
        GMParser.parse(ServerContext.buildDefaultServerContext(), content);
    }

    @Test
    public void garlicMessageWithInvalidSignatureReturnsNull() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        byte[] content = garlicMessageBytes(serverContext, serverContext.getNodeId());
        content[content.length - 1] ^= 0x01;

        GMContent parsed = GMParser.parse(serverContext, content);

        assertNull(parsed);
    }

    @Test
    public void duplicateGarlicMessageIsIgnored() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        byte[] content = garlicMessageBytes(serverContext, serverContext.getNodeId());

        assertNotNull(GMParser.parse(serverContext, content));
        assertNull(GMParser.parse(serverContext, content));
    }

    @Test
    public void ackNotifiesFlaschenpostJob() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        StubFlaschenpostJob job = new StubFlaschenpostJob(serverContext);
        int ackId = 424242;
        registerJob(job, ackId);

        GMContent parsed = GMParser.parse(serverContext, new GMAck(ackId).getContent());

        assertNotNull(parsed);
        assertTrue(job.successCalled);
    }

    @Test
    public void ackNotifiesGarlicJob() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        StubGarlicJob job = new StubGarlicJob(serverContext);
        int ackId = 434343;
        registerJob(job, ackId);

        GMContent parsed = GMParser.parse(serverContext, new GMAck(ackId).getContent());

        assertNotNull(parsed);
        assertTrue(job.successCalled);
    }

    @Test
    public void garlicMessageIsSentToConnectedPeerDirectly() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeId target = NodeId.generateWithSimpleKey();
        byte[] content = garlicMessageBytes(serverContext, target);

        TestPeer peer = new TestPeer("10.0.0.1", 1000, target);
        peer.writeBuffer = ByteBuffer.allocate(content.length + BUFFER_PADDING);
        peer.setConnected(true);
        serverContext.getPeerList().add(peer);

        GMContent parsed = GMParser.parse(serverContext, content);

        assertNotNull(parsed);
        peer.writeBuffer.flip();
        assertEquals(Command.FLASCHENPOST_PUT, peer.writeBuffer.get());
        assertTrue(peer.setWriteBufferCalled);
    }

    @Test
    public void kadContentEntryPointsAreUsed() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeId destination = NodeId.generateWithSimpleKey();
        new Node(serverContext, destination);

        NodeInfoModel nodeInfoModel = new NodeInfoModel();
        NodeId missingPeerId = NodeId.generateWithSimpleKey();
        GMEntryPointModel missingEntryPoint = new GMEntryPointModel(missingPeerId);
        missingEntryPoint.setIp("192.0.2.10");
        missingEntryPoint.setPort(10);
        nodeInfoModel.addEntryPoint(missingEntryPoint);

        NodeId reachablePeerId = NodeId.generateWithSimpleKey();
        GMEntryPointModel reachableEntryPoint = new GMEntryPointModel(reachablePeerId);
        reachableEntryPoint.setIp("192.0.2.11");
        reachableEntryPoint.setPort(11);
        nodeInfoModel.addEntryPoint(reachableEntryPoint);

        KadContent kadContent = new KadContent(System.currentTimeMillis() - Duration.ofMinutes(9).toMillis(),
                destination.exportPublic(), nodeInfoModel.export().getBytes(StandardCharsets.UTF_8));
        kadContent.setId(KadContent.createKademliaId(destination));
        serverContext.getKadStoreManager().put(kadContent);

        byte[] content = garlicMessageBytes(serverContext, destination);

        TestPeer reachablePeer = new TestPeer("192.0.2.11", 11, reachablePeerId);
        reachablePeer.writeBuffer = ByteBuffer.allocate(content.length + BUFFER_PADDING);
        reachablePeer.setConnected(true);
        serverContext.getPeerList().add(reachablePeer);

        GMContent parsed = GMParser.parse(serverContext, content);

        assertNotNull(parsed);
        assertTrue(reachablePeer.setWriteBufferCalled);
    }

    @Test
    public void missingKadContentTriggersSearchAndReturnsWhenNoRoute() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeId destination = NodeId.generateWithSimpleKey();
        new Node(serverContext, destination);

        TestPeer noNodeId = new TestPeer("10.0.0.1", 1);
        noNodeId.setConnected(true);

        TestPeer notConnected = new TestPeer("10.0.0.2", 2, NodeId.generateWithSimpleKey());
        notConnected.setConnected(false);

        TestPeer notAuthed = new TestPeer("10.0.0.3", 3, NodeId.generateWithSimpleKey());
        notAuthed.setConnected(true);
        notAuthed.setNode(new Node(serverContext, notAuthed.getNodeId()));

        TestPeer lightClient = new TestPeer("10.0.0.4", 4, NodeId.generateWithSimpleKey());
        lightClient.authed = true;
        lightClient.setConnected(true);
        lightClient.setNode(new Node(serverContext, lightClient.getNodeId()));
        lightClient.setLightClient(true);

        serverContext.getPeerList().add(noNodeId);
        serverContext.getPeerList().add(notConnected);
        serverContext.getPeerList().add(notAuthed);
        serverContext.getPeerList().add(lightClient);

        GMContent parsed = GMParser.parse(serverContext, garlicMessageBytes(serverContext, destination));

        assertNotNull(parsed);
        assertFalse(lightClient.setWriteBufferCalled);
    }

    @Test
    public void graphRoutingSelectsPeerWithShortestPath() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeId destination = NodeId.generateWithSimpleKey();
        Node destinationNode = new Node(serverContext, destination);

        Node selfNode = new Node(serverContext, serverContext.getNodeId());
        serverContext.setNode(selfNode);

        DefaultDirectedWeightedGraph<Node, NodeEdge> graph = serverContext.getNodeStore().getNodeGraph();
        graph.addVertex(selfNode);
        graph.addVertex(destinationNode);
        NodeEdge edge = graph.addEdge(selfNode, destinationNode);
        graph.setEdgeWeight(edge, 4.0);

        NodeInfoModel nodeInfoModel = new NodeInfoModel();
        KadContent kadContent = new KadContent(System.currentTimeMillis(), destination.exportPublic(),
                nodeInfoModel.export().getBytes(StandardCharsets.UTF_8));
        kadContent.setId(KadContent.createKademliaId(destination));
        serverContext.getKadStoreManager().put(kadContent);

        TestPeer goodPeer = new TestPeer("10.0.0.5", 5, NodeId.generateWithSimpleKey());
        goodPeer.authed = true;
        goodPeer.setConnected(true);
        goodPeer.setNode(new Node(serverContext, goodPeer.getNodeId()));
        byte[] content = garlicMessageBytes(serverContext, destination);
        goodPeer.writeBuffer = ByteBuffer.allocate(content.length + BUFFER_PADDING);
        serverContext.getPeerList().add(goodPeer);

        GMParser.parse(serverContext, content);

        assertTrue(goodPeer.setWriteBufferCalled);
    }

    @Test
    public void graphRoutingSkipsWhenPathMissing() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeId destination = NodeId.generateWithSimpleKey();
        new Node(serverContext, destination);

        TestPeer peer = new TestPeer("10.0.0.6", 6, NodeId.generateWithSimpleKey());
        peer.authed = true;
        peer.setConnected(true);
        peer.setNode(new Node(serverContext, peer.getNodeId()));
        byte[] content = garlicMessageBytes(serverContext, destination);
        peer.writeBuffer = ByteBuffer.allocate(content.length + BUFFER_PADDING);
        serverContext.getPeerList().add(peer);

        GMParser.parse(serverContext, content);

        assertFalse(peer.setWriteBufferCalled);
    }

    @Test
    public void nodeStoreMissesDestinationAndReturnsQuietly() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeId destination = NodeId.generateWithSimpleKey();

        GMContent parsed = GMParser.parse(serverContext, garlicMessageBytes(serverContext, destination));

        assertNotNull(parsed);
    }

    @Test
    public void recentKadContentSkipsRefresh() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeId destination = NodeId.generateWithSimpleKey();
        new Node(serverContext, destination);

        NodeInfoModel nodeInfoModel = new NodeInfoModel();
        NodeId entryPointId = NodeId.generateWithSimpleKey();
        GMEntryPointModel entryPoint = new GMEntryPointModel(entryPointId);
        entryPoint.setIp("203.0.113.10");
        entryPoint.setPort(12345);
        nodeInfoModel.addEntryPoint(entryPoint);

        KadContent kadContent = new KadContent(System.currentTimeMillis() - Duration.ofMinutes(1).toMillis(),
                destination.exportPublic(), nodeInfoModel.export().getBytes(StandardCharsets.UTF_8));
        kadContent.setId(KadContent.createKademliaId(destination));
        serverContext.getKadStoreManager().put(kadContent);

        byte[] content = garlicMessageBytes(serverContext, destination);
        TestPeer entryPeer = new TestPeer("203.0.113.10", 12345, entryPointId);
        entryPeer.setConnected(true);
        entryPeer.writeBuffer = ByteBuffer.allocate(content.length + BUFFER_PADDING);
        serverContext.getPeerList().add(entryPeer);

        GMParser.parse(serverContext, content);

        assertTrue(entryPeer.setWriteBufferCalled);
    }

    @Test
    public void entryPointAddsNodeWhenPeerUnavailable() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeId destination = NodeId.generateWithSimpleKey();
        new Node(serverContext, destination);

        NodeInfoModel nodeInfoModel = new NodeInfoModel();
        NodeId entryPointId = NodeId.generateWithSimpleKey();
        GMEntryPointModel entryPoint = new GMEntryPointModel(entryPointId);
        entryPoint.setIp("198.51.100.10");
        entryPoint.setPort(4242);
        nodeInfoModel.addEntryPoint(entryPoint);

        KadContent kadContent = new KadContent(System.currentTimeMillis(),
                destination.exportPublic(), nodeInfoModel.export().getBytes(StandardCharsets.UTF_8));
        kadContent.setId(KadContent.createKademliaId(destination));
        serverContext.getKadStoreManager().put(kadContent);

        GMParser.parse(serverContext, garlicMessageBytes(serverContext, destination));

        assertNotNull(serverContext.getNodeStore().get(entryPointId.getKademliaId()));
    }

    @Test
    public void entryPointWithDisconnectedPeerFallsBackToAdd() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeId destination = NodeId.generateWithSimpleKey();
        new Node(serverContext, destination);

        NodeInfoModel nodeInfoModel = new NodeInfoModel();
        NodeId entryPointId = NodeId.generateWithSimpleKey();
        GMEntryPointModel entryPoint = new GMEntryPointModel(entryPointId);
        entryPoint.setIp("203.0.113.11");
        entryPoint.setPort(4243);
        nodeInfoModel.addEntryPoint(entryPoint);

        KadContent kadContent = new KadContent(System.currentTimeMillis(),
                destination.exportPublic(), nodeInfoModel.export().getBytes(StandardCharsets.UTF_8));
        kadContent.setId(KadContent.createKademliaId(destination));
        serverContext.getKadStoreManager().put(kadContent);

        TestPeer disconnectedPeer = new TestPeer("203.0.113.11", 4243, entryPointId);
        disconnectedPeer.setConnected(false);
        serverContext.getPeerList().add(disconnectedPeer);

        GMParser.parse(serverContext, garlicMessageBytes(serverContext, destination));

        assertNotNull(serverContext.getNodeStore().get(entryPointId.getKademliaId()));
    }

    @Test
    public void shortestPathBranchFalseWhenEqualWeight() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeId destination = NodeId.generateWithSimpleKey();
        Node destinationNode = new Node(serverContext, destination);

        Node selfNode = new Node(serverContext, serverContext.getNodeId());
        serverContext.setNode(selfNode);

        DefaultDirectedWeightedGraph<Node, NodeEdge> graph = serverContext.getNodeStore().getNodeGraph();
        graph.addVertex(selfNode);
        graph.addVertex(destinationNode);
        NodeEdge edge = graph.addEdge(selfNode, destinationNode);
        graph.setEdgeWeight(edge, 2.0);

        NodeInfoModel nodeInfoModel = new NodeInfoModel();
        KadContent kadContent = new KadContent(System.currentTimeMillis(),
                destination.exportPublic(), nodeInfoModel.export().getBytes(StandardCharsets.UTF_8));
        kadContent.setId(KadContent.createKademliaId(destination));
        serverContext.getKadStoreManager().put(kadContent);

        byte[] content = garlicMessageBytes(serverContext, destination);

        TestPeer firstPeer = new TestPeer("10.0.0.7", 7, NodeId.generateWithSimpleKey());
        firstPeer.authed = true;
        firstPeer.setConnected(true);
        firstPeer.setNode(new Node(serverContext, firstPeer.getNodeId()));
        firstPeer.writeBuffer = ByteBuffer.allocate(content.length + BUFFER_PADDING);

        TestPeer secondPeer = new TestPeer("10.0.0.8", 8, NodeId.generateWithSimpleKey());
        secondPeer.authed = true;
        secondPeer.setConnected(true);
        secondPeer.setNode(new Node(serverContext, secondPeer.getNodeId()));
        secondPeer.writeBuffer = ByteBuffer.allocate(content.length + BUFFER_PADDING);

        serverContext.getPeerList().add(firstPeer);
        serverContext.getPeerList().add(secondPeer);

        GMParser.parse(serverContext, content);

        assertTrue(firstPeer.setWriteBufferCalled || secondPeer.setWriteBufferCalled);
    }

    @Test
    public void existingPeerButDisconnectedTriggersFallbackRouting() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        NodeId destination = NodeId.generateWithSimpleKey();
        TestPeer destinationPeer = new TestPeer("10.0.0.9", 9, destination);
        destinationPeer.setConnected(false);
        serverContext.getPeerList().add(destinationPeer);

        GMContent parsed = GMParser.parse(serverContext, garlicMessageBytes(serverContext, destination));

        assertNotNull(parsed);
    }
    @Test
    public void nullPeerArrayListReturnsGracefully() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        serverContext.setPeerList(new NullPeerList(serverContext));
        NodeId destination = NodeId.generateWithSimpleKey();

        GMContent parsed = GMParser.parse(serverContext, garlicMessageBytes(serverContext, destination));

        assertNotNull(parsed);
    }
}
