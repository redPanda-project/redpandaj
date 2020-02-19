package im.redpanda.core;

import im.redpanda.commands.FBPeer;
import im.redpanda.commands.FBPeerList;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.Security;

import static org.junit.Assert.assertTrue;

public class ParseCommandTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    public Peer getPeerForDebug() {
        Peer me = new Peer("me", 1);
        me.writeBuffer = ByteBuffer.allocate(1024*1024*5);
        return me;
    }

    @Test
    public void testREQUEST_PEERLIST() {

        int peersToTest = 100;

        PeerList.getReadWriteLock().writeLock().lock();
        int i = 0;
        for (i = 0; i < peersToTest; i++) {
            Peer testpeer1 = new Peer("testip" + i, i);
            testpeer1.setNodeId(new NodeId());
            PeerList.add(testpeer1);
        }

        PeerList.getReadWriteLock().writeLock().unlock();

        Peer me = getPeerForDebug();

        ConnectionReaderThread.parseCommand(Command.REQUEST_PEERLIST, null, me);

        ByteBuffer writeBuffer = me.getWriteBuffer();

        System.out.println("" + writeBuffer);

        writeBuffer.flip();

        byte cmd = writeBuffer.get();

        assertTrue(cmd == Command.SEND_PEERLIST);

        int bytesforBuffer = writeBuffer.getInt();

        byte[] bytesForFBPeerList = new byte[bytesforBuffer];

        writeBuffer.get(bytesForFBPeerList);

        FBPeerList rootAsFBPeerList = FBPeerList.getRootAsFBPeerList(ByteBuffer.wrap(bytesForFBPeerList));

        assertTrue(rootAsFBPeerList.peersLength() == peersToTest);

        FBPeer foundPeer = null;
        for (int j = 0; j < rootAsFBPeerList.peersLength(); j++) {
            foundPeer = rootAsFBPeerList.peers(j);
//            System.out.println("" + foundPeer.ip());
        }

        assertTrue(foundPeer.ip().equals("testip" + (i-1)));

        assertTrue(writeBuffer.remaining() == 0);


    }

}
