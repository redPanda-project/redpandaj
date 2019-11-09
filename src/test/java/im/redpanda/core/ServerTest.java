package im.redpanda.core;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ServerTest {

    @Test
    public void findPeer() {

        Server.peerListLock.writeLock().lock();

        Peer peer = new Peer("1.1.1.1", 123);
        Peer peer2 = new Peer("1.1.1.1", 123);
        Peer peer3 = new Peer("1.1.1.1", 124);
        Peer peer4 = new Peer("1.1.1.2", 123);

        Server.findPeer(peer);

        assertTrue(Server.peerList.size() == 1);

        Server.findPeer(peer2);

        assertTrue(Server.peerList.size() == 1);

        Server.findPeer(peer3);

        assertTrue(Server.peerList.size() == 2);

        Server.findPeer(peer4);

        assertTrue(Server.peerList.size() == 3);

        Server.peerListLock.writeLock().unlock();

    }

    @Test
    public void removePeer() {

        Server.peerListLock.writeLock().lock();

        int size = Server.peerList.size();

        Peer peer = new Peer("3.1.234.1", 453543);

        Server.findPeer(peer);

        assertTrue(Server.peerList.size() == size+1);

        Server.removePeer(peer);

        assertTrue(Server.peerList.size() == size);


        Server.peerListLock.writeLock().unlock();


    }
}