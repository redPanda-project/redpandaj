package im.redpanda.core;

import org.junit.Test;

import static org.junit.Assert.*;

public class NodeAdditionalTest {

    @Test
    public void connectionPoints_addSeenAndOrderByEarliest() throws InterruptedException {
        ServerContext ctx = ServerContext.buildDefaultServerContext();
        ctx.setNode(new Node(ctx, ctx.getNodeId()));

        Node n = new Node(ctx, new NodeId());
        assertNull(n.latestSeenConnectionPoint());

        assertTrue(n.addConnectionPoint("127.0.0.1", 1234));
        assertFalse("duplicate add should return false", n.addConnectionPoint("127.0.0.1", 1234));

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
    public void blacklistAndScore_resetsAndCalculates() {
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
