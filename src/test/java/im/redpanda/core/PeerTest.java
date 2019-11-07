package im.redpanda.core;

import org.junit.Test;

import static org.junit.Assert.*;

public class PeerTest {

    static {

        Server.NONCE = new KademliaId();

    }

    @Test
    public void getNodeId() {
    }

    @Test
    public void equalsIpAndPort() {

        Peer peer = new Peer("1.1.1.1", 123);
        Peer peer2 = new Peer("1.1.1.1", 123);
        Peer peer3 = new Peer("1.1.1.2", 123);
        Peer peer4 = new Peer("1.1.1.1", 124);

        assertTrue(peer.equalsIpAndPort(peer2));
        assertFalse(peer.equalsIpAndPort(peer3));
        assertFalse(peer.equalsIpAndPort(peer4));

    }

    @Test
    public void equalsNonce() {

        Peer peer = new Peer("1.1.1.1", 123);
        Peer peer2 = new Peer("1.1.1.1", 123);
        Peer peer3 = new Peer("1.1.1.1", 123);

        KademliaId id1 = new KademliaId();
        KademliaId id2 = new KademliaId();

        assertNotNull(peer);
        assertNotNull(id1);

        peer.setNodeId(id1);
        peer2.setNodeId(id1);
        peer3.setNodeId(id2);

        assertTrue(peer.equalsNonce(peer2));
        assertFalse(peer.equalsNonce(peer3));

    }

    @Test
    public void equalsInstance() {

        Peer peer = new Peer("1.1.1.1", 123);
        Peer peer2 = new Peer("1.1.1.1", 123);

        assertTrue(peer.equalsInstance(peer));
        assertFalse(peer.equalsInstance(peer2));

    }

    @Test
    public void setNodeId() {
        Peer peer = new Peer("1.1.1.1", 123);
        KademliaId id1 = new KademliaId();
//        peer.setNodeId(id1);
//        assertTrue(peer.getNodeId().equals(id1));
    }
}