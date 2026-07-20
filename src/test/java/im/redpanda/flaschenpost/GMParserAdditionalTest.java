package im.redpanda.flaschenpost;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.junit.Test;

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

  @Test
  public void parseUnknownTypeDrops() {
    // An unknown type byte from the network must be dropped (null), not thrown — otherwise the
    // exception unwinds the reader loop and the connection keeps retrying (Sentry REDPANDAJ-2DR).
    byte[] content = new byte[] {(byte) 99, 0, 0, 0};
    assertNull(GMParser.parse(ServerContext.buildDefaultServerContext(), content));
  }

  @Test
  public void parseAckTypeWithGarbagePayloadDrops() {
    // Reproduces REDPANDAJ-2DR: an end-to-end encrypted client payload whose first byte is 0x04
    // (== GMType.ACK) followed by ciphertext. GMAck.parseContent throws on the bad length; parse
    // must catch it, drop the packet and return null instead of unwinding the reader loop.
    byte[] content =
        new byte[] {GMType.ACK.getId(), (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef, 0x2a};
    assertNull(GMParser.parse(ServerContext.buildDefaultServerContext(), content));
  }

  @Test
  public void parseAckTypeOnlyDrops() {
    // A truncated ACK (type byte only, no length/ackid) must also be dropped, not throw.
    byte[] content = new byte[] {GMType.ACK.getId()};
    assertNull(GMParser.parse(ServerContext.buildDefaultServerContext(), content));
  }

  // --- REDPANDAJ-2DR: isValidFrame pre-check used by the FlaschenpostPut legacy path ---

  @Test
  public void isValidFrame_nullOrEmptyContent_isFalse() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    assertFalse(GMParser.isValidFrame(serverContext, null));
    assertFalse(GMParser.isValidFrame(serverContext, new byte[0]));
  }

  @Test
  public void isValidFrame_unknownTypeByte_isFalse() {
    byte[] content = new byte[] {(byte) 99, 0, 0, 0};
    assertFalse(GMParser.isValidFrame(ServerContext.buildDefaultServerContext(), content));
  }

  @Test
  public void isValidFrame_malformedAck_isFalse() {
    // Same shape as the REDPANDAJ-2DR reproduction: an E2E-encrypted payload whose first byte
    // collides with GMType.ACK, followed by ciphertext that is not a valid GMAck body.
    byte[] content =
        new byte[] {GMType.ACK.getId(), (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef, 0x2a};
    assertFalse(GMParser.isValidFrame(ServerContext.buildDefaultServerContext(), content));
  }

  @Test
  public void isValidFrame_truncatedAck_isFalse() {
    byte[] content = new byte[] {GMType.ACK.getId()};
    assertFalse(GMParser.isValidFrame(ServerContext.buildDefaultServerContext(), content));
  }

  @Test
  public void isValidFrame_wellFormedAck_isTrue() {
    ByteBuffer ack = ByteBuffer.allocate(1 + 4 + 4);
    ack.put(GMType.ACK.getId());
    ack.putInt(4);
    ack.putInt(42);
    assertTrue(GMParser.isValidFrame(ServerContext.buildDefaultServerContext(), ack.array()));
  }

  @Test
  public void isValidFrame_wellFormedGarlicMessage_isTrue() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    byte[] content = garlicMessageBytes(serverContext, serverContext.getNodeId());
    assertTrue(GMParser.isValidFrame(serverContext, content));
  }

  @Test
  public void isValidFrame_malformedGarlicMessage_isFalse() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    byte[] content = new byte[] {GMType.GARLIC_MESSAGE.getId(), 0, 0, 0, 5, 1, 2}; // truncated
    assertFalse(GMParser.isValidFrame(serverContext, content));
  }

  @Test
  public void garlicMessageWithTamperedCiphertextYieldsNoContent() {
    // v2 (MS03): a tampered ciphertext fails GCM authentication at decryption time — the
    // packet is dropped without any nested content being parsed.
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    byte[] content = garlicMessageBytes(serverContext, serverContext.getNodeId());
    content[content.length - 1] ^= 0x01;

    GMContent parsed = GMParser.parse(serverContext, content);

    assertNotNull(parsed);
    assertTrue(((GarlicMessage) parsed).getGMContent().isEmpty());
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

    KadContent kadContent =
        new KadContent(
            System.currentTimeMillis() - Duration.ofMinutes(9).toMillis(),
            destination.exportPublic(),
            nodeInfoModel.export().getBytes(StandardCharsets.UTF_8));
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

    GMContent parsed =
        GMParser.parse(serverContext, garlicMessageBytes(serverContext, destination));

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

    DefaultDirectedWeightedGraph<Node, NodeEdge> graph =
        serverContext.getNodeStore().getNodeGraph();
    graph.addVertex(selfNode);
    graph.addVertex(destinationNode);

    NodeInfoModel nodeInfoModel = new NodeInfoModel();
    KadContent kadContent =
        new KadContent(
            System.currentTimeMillis(),
            destination.exportPublic(),
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

    // A real route through goodPeer: self -> goodPeer -> destination. Under correct routing the
    // candidate must have a direct edge from self and a path onward to the target to be selected.
    graph.addVertex(goodPeer.getNode());
    graph.setEdgeWeight(graph.addEdge(selfNode, goodPeer.getNode()), 2.0);
    graph.setEdgeWeight(graph.addEdge(goodPeer.getNode(), destinationNode), 2.0);

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

    GMContent parsed =
        GMParser.parse(serverContext, garlicMessageBytes(serverContext, destination));

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

    KadContent kadContent =
        new KadContent(
            System.currentTimeMillis() - Duration.ofMinutes(1).toMillis(),
            destination.exportPublic(),
            nodeInfoModel.export().getBytes(StandardCharsets.UTF_8));
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

    KadContent kadContent =
        new KadContent(
            System.currentTimeMillis(),
            destination.exportPublic(),
            nodeInfoModel.export().getBytes(StandardCharsets.UTF_8));
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

    KadContent kadContent =
        new KadContent(
            System.currentTimeMillis(),
            destination.exportPublic(),
            nodeInfoModel.export().getBytes(StandardCharsets.UTF_8));
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

    DefaultDirectedWeightedGraph<Node, NodeEdge> graph =
        serverContext.getNodeStore().getNodeGraph();
    graph.addVertex(selfNode);
    graph.addVertex(destinationNode);

    NodeInfoModel nodeInfoModel = new NodeInfoModel();
    KadContent kadContent =
        new KadContent(
            System.currentTimeMillis(),
            destination.exportPublic(),
            nodeInfoModel.export().getBytes(StandardCharsets.UTF_8));
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

    // Two equal-cost real routes (self -> peer -> destination, total 2 each). This exercises the
    // "not strictly less" branch: whichever peer is iterated first stays selected and the other
    // does not replace it.
    graph.addVertex(firstPeer.getNode());
    graph.addVertex(secondPeer.getNode());
    graph.setEdgeWeight(graph.addEdge(selfNode, firstPeer.getNode()), 1.0);
    graph.setEdgeWeight(graph.addEdge(firstPeer.getNode(), destinationNode), 1.0);
    graph.setEdgeWeight(graph.addEdge(selfNode, secondPeer.getNode()), 1.0);
    graph.setEdgeWeight(graph.addEdge(secondPeer.getNode(), destinationNode), 1.0);

    GMParser.parse(serverContext, content);

    assertTrue(firstPeer.setWriteBufferCalled || secondPeer.setWriteBufferCalled);
  }

  /**
   * Regression test for B3: the candidate that yields the cheapest route must be chosen even when
   * it is not the first candidate in iteration order. The previous implementation computed the path
   * {@code self -> target} independent of the candidate, so the first candidate always won.
   *
   * <p>Graph: self has a cheap direct link to peerB (weight 1) and an expensive one to peerA
   * (weight 10); both peers reach the target with weight 1. peerA is iterated first but peerB is
   * the correct (cheaper) next hop.
   */
  @Test
  public void selectBestRoutePeer_picksCheapestRouteNotFirstCandidate() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    Node selfNode = new Node(serverContext, serverContext.getNodeId());
    Node targetNode = new Node(serverContext, NodeId.generateWithSimpleKey());

    TestPeer peerA = authedPeerWithNode(serverContext, "10.1.0.1", 1);
    TestPeer peerB = authedPeerWithNode(serverContext, "10.1.0.2", 2);
    Node peerANode = peerA.getNode();
    Node peerBNode = peerB.getNode();

    DefaultDirectedWeightedGraph<Node, NodeEdge> graph =
        new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    graph.addVertex(selfNode);
    graph.addVertex(targetNode);
    graph.addVertex(peerANode);
    graph.addVertex(peerBNode);

    graph.setEdgeWeight(graph.addEdge(selfNode, peerANode), 10.0);
    graph.setEdgeWeight(graph.addEdge(selfNode, peerBNode), 1.0);
    graph.setEdgeWeight(graph.addEdge(peerANode, targetNode), 1.0);
    graph.setEdgeWeight(graph.addEdge(peerBNode, targetNode), 1.0);

    // peerA iterated first, but peerB (total 1+1=2) beats peerA (total 10+1=11).
    java.util.List<Peer> candidates = java.util.List.of(peerA, peerB);

    GMParser.RouteSelection selection =
        GMParser.selectBestRoutePeer(
            graph, selfNode, candidates, targetNode, GMParser.MAX_ROUTE_WEIGHT);

    assertEquals(peerB, selection.peer());
    assertEquals(2.0, selection.weight(), 1e-9);
  }

  /** A single candidate must be chosen when it has a valid route (behavior parity). */
  @Test
  public void selectBestRoutePeer_singleCandidateIsChosen() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    Node selfNode = new Node(serverContext, serverContext.getNodeId());
    Node targetNode = new Node(serverContext, NodeId.generateWithSimpleKey());

    TestPeer peer = authedPeerWithNode(serverContext, "10.2.0.1", 1);
    Node peerNode = peer.getNode();

    DefaultDirectedWeightedGraph<Node, NodeEdge> graph =
        new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    graph.addVertex(selfNode);
    graph.addVertex(targetNode);
    graph.addVertex(peerNode);
    graph.setEdgeWeight(graph.addEdge(selfNode, peerNode), 3.0);
    graph.setEdgeWeight(graph.addEdge(peerNode, targetNode), 2.0);

    GMParser.RouteSelection selection =
        GMParser.selectBestRoutePeer(
            graph, selfNode, java.util.List.of(peer), targetNode, GMParser.MAX_ROUTE_WEIGHT);

    assertEquals(peer, selection.peer());
    assertEquals(5.0, selection.weight(), 1e-9);
  }

  /** No candidate has a known direct edge from self → none is selected. */
  @Test
  public void selectBestRoutePeer_returnsNullWhenNoDirectEdge() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    Node selfNode = new Node(serverContext, serverContext.getNodeId());
    Node targetNode = new Node(serverContext, NodeId.generateWithSimpleKey());

    TestPeer peer = authedPeerWithNode(serverContext, "10.3.0.1", 1);
    Node peerNode = peer.getNode();

    DefaultDirectedWeightedGraph<Node, NodeEdge> graph =
        new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    graph.addVertex(selfNode);
    graph.addVertex(targetNode);
    graph.addVertex(peerNode);
    // Only an edge from the peer onward — no self -> peer edge.
    graph.setEdgeWeight(graph.addEdge(peerNode, targetNode), 1.0);

    GMParser.RouteSelection selection =
        GMParser.selectBestRoutePeer(
            graph, selfNode, java.util.List.of(peer), targetNode, GMParser.MAX_ROUTE_WEIGHT);

    assertNull(selection.peer());
  }

  /**
   * A null target (destination unknown to the NodeStore) must yield no selection instead of
   * crashing inside Dijkstra with a NullPointerException.
   */
  @Test
  public void selectBestRoutePeer_nullTargetYieldsNoSelection() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    Node selfNode = new Node(serverContext, serverContext.getNodeId());
    TestPeer peer = authedPeerWithNode(serverContext, "10.4.0.1", 1);
    Node peerNode = peer.getNode();

    DefaultDirectedWeightedGraph<Node, NodeEdge> graph =
        new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    graph.addVertex(selfNode);
    graph.addVertex(peerNode);
    graph.setEdgeWeight(graph.addEdge(selfNode, peerNode), 1.0);

    GMParser.RouteSelection selection =
        GMParser.selectBestRoutePeer(
            graph, selfNode, java.util.List.of(peer), null, GMParser.MAX_ROUTE_WEIGHT);

    assertNull(selection.peer());
  }

  /**
   * Regression for REDPANDAJ-2DW/2E5-class races: if an edge on the candidate's remaining path is
   * concurrently ripped out of the graph (maintainNodes on another thread) while Dijkstra is
   * traversing it, jgrapht's intrusive edge specifics throw a {@link NullPointerException} rather
   * than {@link IllegalArgumentException}. Before the fix, {@code selectBestRoutePeer} only caught
   * {@code IllegalArgumentException} around {@code findPathBetween}, so that NPE escaped past the
   * job and crashed the caller instead of the candidate simply being skipped.
   */
  @Test
  public void selectBestRoutePeer_survivesEdgeVanishingMidDijkstraTraversal() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    Node selfNode = new Node(serverContext, serverContext.getNodeId());
    Node targetNode = new Node(serverContext, NodeId.generateWithSimpleKey());
    TestPeer peer = authedPeerWithNode(serverContext, "10.5.0.1", 1);
    Node peerNode = peer.getNode();

    DefaultDirectedWeightedGraph<Node, NodeEdge> realGraph =
        new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    realGraph.addVertex(selfNode);
    realGraph.addVertex(targetNode);
    realGraph.addVertex(peerNode);
    realGraph.setEdgeWeight(realGraph.addEdge(selfNode, peerNode), 1.0);
    realGraph.setEdgeWeight(realGraph.addEdge(peerNode, targetNode), 1.0);

    // Wrap the graph so that, exactly like a concurrent removeEdge/removeVertex would,
    // getEdgeWeight throws NullPointerException the moment Dijkstra tries to weigh the
    // peer -> target edge mid-traversal.
    org.jgrapht.graph.GraphDelegator<Node, NodeEdge> flakyGraph =
        new org.jgrapht.graph.GraphDelegator<>(realGraph) {
          @Override
          public double getEdgeWeight(NodeEdge e) {
            if (realGraph.getEdgeSource(e).equals(peerNode)) {
              throw new NullPointerException("edge vanished mid-traversal");
            }
            return super.getEdgeWeight(e);
          }
        };

    GMParser.RouteSelection selection =
        GMParser.selectBestRoutePeer(
            flakyGraph, selfNode, java.util.List.of(peer), targetNode, GMParser.MAX_ROUTE_WEIGHT);

    // The NPE must be swallowed (candidate skipped), not escape the call.
    assertNull(selection.peer());
  }

  /**
   * Builds an authed + connected peer with a Node attached, so {@link Peer#getNode()} returns the
   * node (it returns null for un-authed / disconnected peers).
   */
  private static TestPeer authedPeerWithNode(ServerContext serverContext, String ip, int port) {
    TestPeer peer = new TestPeer(ip, port, NodeId.generateWithSimpleKey());
    peer.authed = true;
    peer.setConnected(true);
    peer.setNode(new Node(serverContext, peer.getNodeId()));
    return peer;
  }

  @Test
  public void existingPeerButDisconnectedTriggersFallbackRouting() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    NodeId destination = NodeId.generateWithSimpleKey();
    TestPeer destinationPeer = new TestPeer("10.0.0.9", 9, destination);
    destinationPeer.setConnected(false);
    serverContext.getPeerList().add(destinationPeer);

    GMContent parsed =
        GMParser.parse(serverContext, garlicMessageBytes(serverContext, destination));

    assertNotNull(parsed);
  }

  @Test
  public void nullPeerArrayListReturnsGracefully() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    serverContext.setPeerList(new NullPeerList(serverContext));
    NodeId destination = NodeId.generateWithSimpleKey();

    GMContent parsed =
        GMParser.parse(serverContext, garlicMessageBytes(serverContext, destination));

    assertNotNull(parsed);
  }
}
