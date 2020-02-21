package im.redpanda.core;

import org.junit.Test;

import java.io.IOException;

public class PeerInHandshakeTest {

    @Test
    public void addConnection() throws IOException, InterruptedException {

//        Log.LEVEL = 10000;
//
//        ConnectionHandler connectionHandler = new ConnectionHandler();
//        connectionHandler.start();
//
//
//        //lets block the main selector worker
//        connectionHandler.selectorLock.lock();
//        connectionHandler.selector.wakeup();
//
//
//        SocketChannel open = SocketChannel.open();
//        open.configureBlocking(false);
//
//        Thread.sleep(100);
//
//        boolean alreadyConnected = open.connect(new InetSocketAddress("127.0.0.1", Server.MY_PORT));
//
//        PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", open);
//
//
//        //lets not read the data by the main thread by using the alreadyConnected value false....
//        peerInHandshake.addConnection(false);
//
//        int cnt = 0;
//        while (cnt < 100) {
//            cnt++;
//            int select = connectionHandler.selector.select();
////            System.out.println("select: " + select);
//            if (select != 0) {
//                break;
//            }
//        }
//
//        Set<SelectionKey> selectionKeys = connectionHandler.selector.selectedKeys();
//
//        assertFalse(selectionKeys.isEmpty());
//
//        assertTrue(selectionKeys.size() == 2);
//
//        for (SelectionKey key : selectionKeys) {
//            if (key.channel() instanceof ServerSocketChannel) {
//                continue;
//            }
//            assertTrue(key.isConnectable());
//        }
//
//
//        open.finishConnect();


//        peerInHandshake.getKey().interestOps(0);
//        connectionHandler.selector.wakeup();
//
//        //lets the main selector accept the connection and disconnect because we are connecting to ourselves
//        connectionHandler.selectorLock.unlock();
//
//        Thread.sleep(2000);
//
//
//        cnt = 0;
//        while (cnt < 10) {
//            cnt++;
//            int select = connectionHandler.selector.select(5);
//            System.out.println("select: " + select);
//            assertTrue(select == 0);
//            if (select != 0) {
//                break;
//            }
//        }

//        selectionKeys = connectionHandler.selector.selectedKeys();
//
//
//        for (SelectionKey key : selectionKeys) {
//            if (key.channel() instanceof ServerSocketChannel) {
//                continue;
//            }
//            assertTrue(key.isReadable());
//
//            ByteBuffer readBuffer = ByteBuffer.allocate(1024);
//
//            open.read(readBuffer);
//
//            assertTrue(ConnectionReaderThread.parseHandshake(peerInHandshake, readBuffer));
//
//            byte[] bytes = new byte[KademliaId.ID_LENGTH];
//            KademliaId zeorByteKadId = KademliaId.fromFirstBytes(bytes);
//
//            assertTrue(peerInHandshake.getIdentity().equals(zeorByteKadId));
//
//        }
//
//
//        Server.connectionHandler.selectorLock.unlock();

    }
}