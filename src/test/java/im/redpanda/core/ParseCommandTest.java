package im.redpanda.core;

import im.redpanda.commands.FBPeer;
import im.redpanda.commands.FBPeerList;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.Security;

import static org.junit.Assert.*;

public class ParseCommandTest {

    private final ServerContext serverContext = new ServerContext();

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    public Peer getPeerForDebug() {
        Peer me = new Peer("me", 1);
        me.setNodeId(new NodeId());
        me.writeBuffer = ByteBuffer.allocate(1024 * 1024 * 5);
        return me;
    }


    @Test
    public void testLoopCommands() {

        ServerContext serverContext = new ServerContext();
        ConnectionReaderThread connectionReaderThread = new ConnectionReaderThread(serverContext, 5);



        //lets check if it is able to parse 3 ping commands in one step
        ByteBuffer allocate = ByteBuffer.allocate(1024);
        allocate.put(Command.PING);
        allocate.put(Command.PING);
        allocate.put(Command.PING);

        Peer peerForDebug = getPeerForDebug();
        serverContext.getPeerList().add(peerForDebug);
        peerForDebug.setConnected(true);
        connectionReaderThread.loopCommands(peerForDebug, allocate);

        //lets go to read mode and check for remaining bytes
        allocate.flip();
        assertFalse(allocate.hasRemaining());


        //lets check a not complete SEND PEERLIST command
        allocate = ByteBuffer.allocate(1024);
        allocate.put(Command.SEND_PEERLIST);
        allocate.putInt(1);

        connectionReaderThread.loopCommands(peerForDebug, allocate);

        //lets go to read mode and check for remaining bytes
        allocate.flip();

        assertTrue(allocate.get() == Command.SEND_PEERLIST);
        assertTrue(allocate.getInt() == 1);


        //lets combine both from above
        allocate = ByteBuffer.allocate(1024);
        allocate.put(Command.PING);
        allocate.put(Command.PING);
        allocate.put(Command.PING);
        allocate.put(Command.PING);
        allocate.put(Command.SEND_PEERLIST);
        allocate.putInt(1);

        peerForDebug.setConnected(true);
        connectionReaderThread.loopCommands(peerForDebug, allocate);

        //lets go to read mode and check for remaining bytes
        allocate.flip();

        assertTrue(allocate.get() == Command.SEND_PEERLIST);
        assertTrue(allocate.getInt() == 1);
    }

    @Test
    public void testREQUEST_PEERLIST() {
        ServerContext serverContext = new ServerContext();
        ConnectionReaderThread connectionReaderThread = new ConnectionReaderThread(serverContext, 5);
        PeerList peerList = serverContext.getPeerList();


        int peersToTest = 100;


        int startingPeerListSize = peerList.size();

        int i = 0;
        for (i = 0; i < peersToTest; i++) {
            Peer testpeer1 = new Peer("rand_rewrewR_testip" + i, i);
            testpeer1.setNodeId(new NodeId());
            peerList.add(testpeer1);
        }


        Peer me = getPeerForDebug();

        connectionReaderThread.parseCommand(Command.REQUEST_PEERLIST, null, me);

        ByteBuffer writeBuffer = me.getWriteBuffer();

//        System.out.println("" + writeBuffer);

        writeBuffer.flip();

        byte cmd = writeBuffer.get();

        assertTrue(cmd == Command.SEND_PEERLIST);

        int bytesforBuffer = writeBuffer.getInt();

        byte[] bytesForFBPeerList = new byte[bytesforBuffer];

        writeBuffer.get(bytesForFBPeerList);

        FBPeerList rootAsFBPeerList = FBPeerList.getRootAsFBPeerList(ByteBuffer.wrap(bytesForFBPeerList));

        assertTrue(rootAsFBPeerList.peersLength() == peerList.size());

        FBPeer foundPeer = null;
        for (int j = 0; j < rootAsFBPeerList.peersLength(); j++) {
            foundPeer = rootAsFBPeerList.peers(j);
//            System.out.println("" + foundPeer.ip());
        }

        assertTrue(foundPeer.ip().equals("rand_rewrewR_testip" + (i - 1)));

        assertTrue(writeBuffer.remaining() == 0);


        //cleanup
        for (i = 0; i < peersToTest; i++) {
            peerList.removeIpPort("rand_rewrewR_testip" + i, i);
        }

        assertEquals(startingPeerListSize, peerList.size());


    }

    @Test
    public void testSend_PEERLIST() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        ConnectionReaderThread connectionReaderThread = new ConnectionReaderThread(serverContext, 5);
        PeerList peerList = serverContext.getPeerList();

        int peersToTest = 100;


        int initPeerListSize = peerList.size();

        int i = 0;
        for (i = 0; i < peersToTest; i++) {
            Peer testpeer1 = new Peer("rand_dwhrgfwer_testip" + i, i);
            testpeer1.setNodeId(new NodeId());
//            System.out.println("node id: " + testpeer1.getNodeId().getKademliaId().toString());
            peerList.add(testpeer1);
        }
//        PeerList.getReadWriteLock().writeLock().unlock();

        Peer me = getPeerForDebug();

        connectionReaderThread.parseCommand(Command.REQUEST_PEERLIST, null, me);

//        PeerList.getReadWriteLock().writeLock().lock();

        peerList.clear();


        ByteBuffer writeBuffer = me.getWriteBuffer();


        writeBuffer.flip();

        // prints bytes of flatbuffer object to console...
        writeBuffer.get();
        int toreadbytes = writeBuffer.getInt();
        byte[] bytes = new byte[toreadbytes];
        writeBuffer.get(bytes);
//        System.out.println("" + Utils.bytesToHexString(bytes));
        writeBuffer.position(0);

        connectionReaderThread.parseCommand(writeBuffer.get(), writeBuffer, getPeerForDebug());

        assertFalse(writeBuffer.hasRemaining());

        assertTrue(peerList.size() - initPeerListSize == peersToTest);


    }

}
