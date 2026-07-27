package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.Security;
import org.junit.Test;

public class PeerInHandshakeTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  //    @Test
  //    public void addConnection() throws IOException, InterruptedException {
  //
  //        ServerContext serverContext = new ServerContext();
  //        ConnectionReaderThread connectionReaderThread = new
  // ConnectionReaderThread(serverContext, 5);
  //        PeerList peerList = serverContext.getPeerList();
  //
  //        //Todo: the tests have to be adapted to the new test system running an redpanda
  // instance...
  //
  //        Log.LEVEL = 10000;
  //
  //        ConnectionHandler connectionHandler = new ConnectionHandler(serverContext,false);
  //        connectionHandler.start();
  //
  ////        ConnectionHandler connectionHandler = Server.connectionHandler;
  //
  //
  //        //lets block the main selector worker
  //        connectionHandler.selectorLock.lock();
  //        try {
  //            connectionHandler.selector.wakeup();
  //
  //
  //            SocketChannel open = SocketChannel.open();
  //            open.configureBlocking(false);
  //
  //            while (Server.MY_PORT == -1) {
  //                Thread.sleep(200);
  //            }
  //
  //            boolean alreadyConnected = open.connect(new InetSocketAddress("127.0.0.1",
  // Server.MY_PORT));
  //
  //            PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", open);
  //
  //
  //            //lets not read the data by the main thread by using the alreadyConnected value
  // false....
  //            peerInHandshake.addConnection(false);
  //
  //            int cnt = 0;
  //            while (cnt < 100) {
  //                cnt++;
  //                int select = connectionHandler.selector.select();
  ////            System.out.println("select: " + select);
  //                if (select != 0) {
  //                    break;
  //                }
  //            }
  //
  //            Set<SelectionKey> selectionKeys = connectionHandler.selector.selectedKeys();
  //
  //
  //            assertFalse(selectionKeys.isEmpty());
  //
  ////        assertTrue(selectionKeys.size() == 2);
  //
  ////        for (SelectionKey key : selectionKeys) {
  ////            if (key.channel() instanceof ServerSocketChannel) {
  ////                continue;
  ////            }
  ////            assertTrue(key.isConnectable());
  ////        }
  //
  //
  //            try {
  //                open.finishConnect();
  //            } catch (ConnectException e) {
  //                e.printStackTrace();
  //            }
  //
  //
  ////        peerInHandshake.getKey().interestOps(0);
  ////        connectionHandler.selector.wakeup();
  ////
  ////        //lets the main selector accept the connection and disconnect because we are
  // connecting to ourselves
  ////        connectionHandler.selectorLock.unlock();
  ////
  ////        Thread.sleep(2000);
  ////
  ////
  ////        cnt = 0;
  ////        while (cnt < 10) {
  ////            cnt++;
  ////            int select = connectionHandler.selector.select(5);
  ////            System.out.println("select: " + select);
  ////            assertTrue(select == 0);
  ////            if (select != 0) {
  ////                break;
  ////            }
  ////        }
  //
  ////        selectionKeys = connectionHandler.selector.selectedKeys();
  ////
  ////
  ////        for (SelectionKey key : selectionKeys) {
  ////            if (key.channel() instanceof ServerSocketChannel) {
  ////                continue;
  ////            }
  ////            assertTrue(key.isReadable());
  ////
  ////            ByteBuffer readBuffer = ByteBuffer.allocate(1024);
  ////
  ////            open.read(readBuffer);
  ////
  ////            assertTrue(ConnectionReaderThread.parseHandshake(peerInHandshake, readBuffer));
  ////
  ////            byte[] bytes = new byte[KademliaId.ID_LENGTH];
  ////            KademliaId zeorByteKadId = KademliaId.fromFirstBytes(bytes);
  ////
  ////            assertTrue(peerInHandshake.getIdentity().equals(zeorByteKadId));
  ////
  ////        }
  ////
  ////
  ////        Server.connectionHandler.selectorLock.unlock();
  //        } finally {
  ////            connectionHandler.selectorLock.unlock();
  //        }
  //
  //
  //    }

  @Test
  public void hasPublicKey() {
    Peer peerWithPublicKey = new Peer("ip", 0);
    peerWithPublicKey.setNodeId(new NodeId());
    PeerInHandshake phWithPublicKey = new PeerInHandshake("ip", peerWithPublicKey, null);

    assertTrue(phWithPublicKey.hasPublicKey());

    Peer peerWithoutPublicKey = new Peer("ip", 0);
    PeerInHandshake phWithoutPublicKey = new PeerInHandshake("ip", peerWithoutPublicKey, null);

    assertFalse(phWithoutPublicKey.hasPublicKey());

    Peer peerWithoutPublicKey2 = new Peer("ip", 0);
    peerWithoutPublicKey2.setNodeId(new NodeId(new KademliaId()));
    PeerInHandshake phWithoutPublicKey2 = new PeerInHandshake("ip", peerWithoutPublicKey2, null);

    assertFalse(phWithoutPublicKey2.hasPublicKey());
  }

  /**
   * Regression for M4 (bug hunt 2026-07-26): when the registration of a freshly opened outgoing
   * channel fails, {@code addConnection} used to call {@code peer.disconnect(...)}, which closes
   * {@code peer.socketChannel} — still null at that point, since the channel is only handed over to
   * the Peer once the handshake completed. The channel that actually failed to register stayed open
   * forever.
   */
  @Test
  public void addConnection_closesItsOwnChannelWhenRegistrationFails() throws Exception {
    Selector originalSelector = ConnectionHandler.selector;
    Selector closedSelector = Selector.open();
    closedSelector.close();

    SocketChannel channel = SocketChannel.open();
    Peer peer = new Peer("127.0.0.1", 1234);
    PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", peer, channel);

    ConnectionHandler.selector = closedSelector;
    try {
      // registering with a closed selector fails; the exception itself may propagate, but the
      // channel must not survive it
      Throwable thrown = catchThrowable(() -> peerInHandshake.addConnection(false));

      assertThat(thrown).isNotNull();
      assertThat(channel.isOpen()).isFalse();
    } finally {
      ConnectionHandler.selector = originalSelector;
      channel.close();
    }
  }

  /**
   * Regression for T68 (a): {@code addConnection} used to return void, so {@code
   * OutboundHandler.connectTo} reported success even when the registration had failed and the peer
   * had already been disconnected again — {@code run()}'s {@code newConnections += 5} backoff
   * pacing therefore never fired for those attempts.
   */
  @Test
  public void addConnection_returnsFalseWhenRegistrationFails() throws Exception {
    SocketChannel channel = SocketChannel.open();
    channel.close(); // configureBlocking() on a closed channel throws ClosedChannelException

    Peer peer = new Peer("127.0.0.1", 1234);
    PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", peer, channel);

    assertFalse(peerInHandshake.addConnection(false));
  }

  @Test
  public void addConnection_returnsTrueWhenRegistrationSucceeds() throws Exception {
    SocketChannel channel = SocketChannel.open();
    Peer peer = new Peer("127.0.0.1", 1234);
    PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", peer, channel);

    try {
      assertTrue(peerInHandshake.addConnection(false));
    } finally {
      if (peerInHandshake.getKey() != null) {
        peerInHandshake.getKey().cancel();
      }
      channel.close();
    }
  }
}
