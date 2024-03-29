package im.redpanda.core;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.Security;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ParseCommandTest {

    private final ServerContext serverContext = new ServerContext();

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        ByteBufferPool.init();
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
        assertThat(allocate.hasRemaining(), is(false));


        //lets check a not complete SEND PEERLIST command
        allocate = ByteBuffer.allocate(1024);
        allocate.put(Command.SEND_PEERLIST);
        allocate.putInt(1);

        connectionReaderThread.loopCommands(peerForDebug, allocate);

        //lets go to read mode and check for remaining bytes
        allocate.flip();

        assertThat(allocate.get() == Command.SEND_PEERLIST, is(true));
        assertThat(allocate.getInt() == 1, is(true));


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

        assertThat(allocate.get(), is(Command.SEND_PEERLIST));
        assertThat(allocate.getInt(), is(1));
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
            testpeer1.setConnected(true);
            peerList.add(testpeer1);
        }


        Peer me = getPeerForDebug();

        connectionReaderThread.parseCommand(Command.REQUEST_PEERLIST, null, me);

        ByteBuffer writeBuffer = me.getWriteBuffer();

//        System.out.println("" + writeBuffer);

        writeBuffer.flip();

        byte cmd = writeBuffer.get();

        assertThat(cmd, is(Command.SEND_PEERLIST));

        int bytesforBuffer = writeBuffer.getInt();

        byte[] bytesForFBPeerList = new byte[bytesforBuffer];

        writeBuffer.get(bytesForFBPeerList);

        ByteBuffer peerListBytes = ByteBuffer.wrap(bytesForFBPeerList);


        int peerListSize = peerListBytes.getInt();


        assertThat(peerListSize, is(peerList.size()));


        for (int j = 0; j < peerListSize; j++) {
            NodeId nodeId = null;
            int booleanNodeIdPresent = peerListBytes.getShort();
            if (booleanNodeIdPresent == 1) {
                byte[] bytes = new byte[NodeId.PUBLIC_KEYLEN];
                peerListBytes.get(bytes);
                nodeId = NodeId.importPublic(bytes);
            }
            String ip = ConnectionReaderThread.parseString(peerListBytes);
            int port = peerListBytes.getInt();

            assertThat(ip, is("rand_rewrewR_testip" + j));
            assertThat(port, is(j));
        }

//        FBPeerList rootAsFBPeerList = FBPeerList.getRootAsFBPeerList(ByteBuffer.wrap(bytesForFBPeerList));
//
//        assertTrue(rootAsFBPeerList.peersLength() == peerList.size());
//
//        FBPeer foundPeer = null;
//        for (int j = 0; j < rootAsFBPeerList.peersLength(); j++) {
//            foundPeer = rootAsFBPeerList.peers(j);
////            System.out.println("" + foundPeer.ip());
//        }

//        assertTrue(foundPeer.ip().equals("rand_rewrewR_testip" + (i - 1)));

        assertThat(writeBuffer.remaining(), is(0));


        //cleanup
        for (i = 0; i < peersToTest; i++) {
            peerList.removeIpPort("rand_rewrewR_testip" + i, i);
        }

        assertThat(startingPeerListSize, is(peerList.size()));


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
            testpeer1.setConnected(true);
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

        assertThat(writeBuffer.hasRemaining(), is(false));

        assertThat(peerList.size() - initPeerListSize, is(peersToTest));


    }

}
