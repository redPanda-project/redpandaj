package im.redpanda.core;

import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    public void remove() throws InterruptedException {

        PeerList peerList = new PeerList();

        Peer toRemovePeerIp = new Peer("toRemovePeerIp", 5);


        int initSize = peerList.size();
        peerList.add(toRemovePeerIp);

        assertTrue(peerList.size() - initSize == 1);

        peerList.remove(toRemovePeerIp);


        assertTrue(peerList.size() - initSize == 0);

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


    @Test
    public void getGoodPeerSimpleTest() {
        PeerList peerList = new PeerList();
        peerList.add(new Peer("127.0.0.1", 50558));

        Peer goodPeer = peerList.getGoodPeer();
        assertNotNull(goodPeer);
    }
}