package im.redpanda.core;

import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class PeerListTest {

    @Test
    public void add() throws InterruptedException {

        PeerList peerList = new PeerList();

        Peer mtestip = new Peer("mtestip", 5);

        boolean b = peerList.getReadWriteLock().writeLock().tryLock(5, TimeUnit.SECONDS);

        if (!b) {
            ThreadInfo[] threads = ManagementFactory.getThreadMXBean()
                    .dumpAllThreads(true, true);
            for (ThreadInfo info : threads) {
                System.out.print(info);
            }
            System.out.println("lock not possible for add test");
            return;
        }

        int initSize = peerList.size();
        peerList.add(mtestip);

        assertTrue(peerList.size() - initSize == 1);
        peerList.add(mtestip);
        assertTrue(peerList.size() - initSize == 1);

        peerList.getReadWriteLock().writeLock().unlock();


    }

    @Test
    public void addWithSameKadId() throws InterruptedException {
        PeerList peerList = new PeerList();

        //different Ips but same KadId
        KademliaId kademliaId = new KademliaId();
        NodeId nodeId = new NodeId(kademliaId);

        Peer peerWithKadId1 = new Peer("mtestip1", 5, nodeId);
        Peer peerWithKadId2 = new Peer("mtestip2", 6, nodeId);


        int initSize = peerList.size();
        peerList.add(peerWithKadId1);

        assertTrue(peerList.size() - initSize == 1);
        peerList.add(peerWithKadId2);
        assertTrue(peerList.size() - initSize == 1);

    }

    @Test
    public void remove() {

        PeerList peerList = new PeerList();

        Peer toRemovePeerIp = new Peer("toRemovePeerIp", 5);


        int initSize = peerList.size();
        peerList.add(toRemovePeerIp);

        assertTrue(peerList.size() - initSize == 1);

        peerList.remove(toRemovePeerIp);


        assertTrue(peerList.size() - initSize == 0);

    }

    @Test
    public void removeByKademliaId() {
        PeerList peerList = new PeerList();
        Peer peer = new Peer("127.0.0.2", 50558);
        NodeId id = new NodeId();
        peer.setNodeId(id);
        peerList.add(peer);

        Peer peer2 = new Peer("127.0.0.1", 50558);
        NodeId id2 = new NodeId();
        peer.setNodeId(id2);
        peerList.add(peer2);

        peerList.remove(id.getKademliaId());

        assertEquals(1, peerList.size());
        assertNotEquals(peer, peerList.getGoodPeer());
    }

    @Test
    public void removeIpPort() {
        PeerList peerList = new PeerList();
        peerList.add(new Peer("127.0.0.1", 50558));
        peerList.removeIpPort("127.0.0.1", 50558);
        assertEquals(0, peerList.size());
    }

    @Test
    public void removeIpPortOnly() {
        PeerList peerList = new PeerList();
        peerList.add(new Peer("127.0.0.1", 50558));
        peerList.removeIpPortOnly("127.0.0.1", 50558);
        assertEquals(1, peerList.size());
    }

    @Test
    public void size() {
        PeerList peerList = new PeerList();
        peerList.add(new Peer("127.0.0.1", 50558));
        assertEquals(1, peerList.size());
    }


    @Test
    public void clear() {
        PeerList peerList = new PeerList();
        peerList.add(new Peer("127.0.0.1", 50558));
        peerList.clear();
        assertEquals(0, peerList.size());
    }

    @Test
    public void updateKademliaId() {
        PeerList peerList = new PeerList();

        Peer peer = new Peer("127.0.0.1", 50558);
        NodeId oldId = new NodeId();
        peer.setNodeId(oldId);
        peerList.add(peer);

        assertEquals(1, peerList.size());
        KademliaId newId = new KademliaId();
        peerList.updateKademliaId(peer, newId);

        assertEquals(peer, peerList.get(newId));
        assertEquals(peer.getKademliaId(), newId);
        assertNotEquals(peer.getKademliaId(), oldId.getKademliaId());
    }

    @Test
    public void getGoodPeer() {
        PeerList peerList = new PeerList();
        peerList.add(new Peer("127.0.0.1", 50558));
        Peer goodPeer = peerList.getGoodPeer();
        assertNotNull(goodPeer);
    }

    @Test
    public void clearConnectionDetails() {
    }

    @Test
    public void get() {
        PeerList peerList = new PeerList();
        Peer peer = new Peer("127.0.0.2", 50558);
        NodeId id = new NodeId();
        peer.setNodeId(id);
        peerList.add(peer);

        Peer peer2 = new Peer("127.0.0.1", 50558);
        NodeId id2 = new NodeId();
        peer.setNodeId(id2);
        peerList.add(peer2);

        assertEquals(peer, peerList.get(id.getKademliaId()));
    }

}