package im.redpanda.core;

import org.junit.Test;

import static org.junit.Assert.*;

public class PeerListTest {

    @Test
    public void add() {

        Peer mtestip = new Peer("mtestip", 5);

        PeerList.getReadWriteLock().writeLock().lock();

        int initSize = PeerList.size();
        PeerList.add(mtestip);

        assertTrue(PeerList.size() - initSize == 1);
        PeerList.add(mtestip);
        assertTrue(PeerList.size() - initSize == 1);

        PeerList.getReadWriteLock().writeLock().unlock();


    }

    @Test
    public void addWithSameKadId() {
        //different Ips but same KadId
        KademliaId kademliaId = new KademliaId();
        NodeId nodeId = new NodeId(kademliaId);

        Peer peerWithKadId1 = new Peer("mtestip1", 5, nodeId);
        Peer peerWithKadId2 = new Peer("mtestip2", 6, nodeId);

        PeerList.getReadWriteLock().writeLock().lock();

        int initSize = PeerList.size();
        PeerList.add(peerWithKadId1);

        assertTrue(PeerList.size() - initSize == 1);
        PeerList.add(peerWithKadId2);
        assertTrue(PeerList.size() - initSize == 1);

        PeerList.getReadWriteLock().writeLock().unlock();
    }

    @Test
    public void remove() {

        Peer toRemovePeerIp = new Peer("toRemovePeerIp", 5);

        PeerList.getReadWriteLock().writeLock().lock();

        int initSize = PeerList.size();
        PeerList.add(toRemovePeerIp);

        assertTrue(PeerList.size() - initSize == 1);

        PeerList.remove(toRemovePeerIp);


        assertTrue(PeerList.size() - initSize == 0);

        PeerList.getReadWriteLock().writeLock().unlock();

    }

    @Test
    public void removeIpPort() {
    }

    @Test
    public void removeIpPortOnly() {
    }

    @Test
    public void testRemove() {
    }

    @Test
    public void get() {
    }

    @Test
    public void size() {
    }
}