package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class NodeAdditionalTest {

  @Test
  void connectionPoints_addSeenAndOrderByEarliest() throws Exception {
    ServerContext ctx = ServerContext.buildDefaultServerContext();
    ctx.setNode(new Node(ctx, ctx.getNodeId()));

    Node n = new Node(ctx, new NodeId());
    assertNull(n.latestSeenConnectionPoint());

    assertTrue(n.addConnectionPoint("127.0.0.1", 1234));
    assertFalse(n.addConnectionPoint("127.0.0.1", 1234), "duplicate add should return false");

    n.seen("10.0.0.1", 1111);
    Thread.sleep(2);
    n.seen("10.0.0.2", 2222);

    assertNotNull(n.latestSeenConnectionPoint());
    // Implementation sorts ascending by lastSeen and returns first (earliest)
    assertEquals(1234, n.latestSeenConnectionPoint().getPort());
    assertEquals("127.0.0.1", n.latestSeenConnectionPoint().getIp());

    int retries = n.incrRetry("10.0.0.2", 2222);
    assertEquals(1, retries);
  }

  @Test
  void blacklistAndScore_resetsAndCalculates() {
    ServerContext ctx = ServerContext.buildDefaultServerContext();
    ctx.setNode(new Node(ctx, ctx.getNodeId()));
    Node n = new Node(ctx, new NodeId());

    n.setGmTestsSuccessful(5);
    n.setGmTestsFailed(3);
    int scoreBefore = n.getScore();

    n.touchBlacklisted();
    assertTrue(n.isBlacklisted());
    assertEquals(0, n.getGmTestsFailed());
    assertEquals(0, n.getGmTestsSuccessful());

    n.resetBlacklisted();
    assertFalse(n.isBlacklisted());

    // Set again and ensure score reflects values
    n.setGmTestsSuccessful(2);
    n.setGmTestsFailed(1);
    int scoreAfter = n.getScore();

    assertNotEquals(scoreBefore, scoreAfter);
  }
}
