package im.redpanda.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ConnectionHandlerTest {

    private ServerContext serverContext;
    private Set<ServerSocketChannel> channelsToClose;

    @Before
    public void setUp() {
        serverContext = ServerContext.buildDefaultServerContext();
        channelsToClose = new HashSet<>();
    }

    @After
    public void tearDown() throws Exception {
        // Clean up any channels we registered with the shared selector so other tests are unaffected.
        for (SelectionKey key : ConnectionHandler.selector.keys()) {
            if (key.channel() instanceof ServerSocketChannel) {
                key.cancel();
            }
        }
        for (ServerSocketChannel channel : channelsToClose) {
            channel.close();
        }

        ConnectionHandler.peerInHandshakes.clear();
    }

    @Test
    public void bindToNextAvailablePortSkipsOccupiedPort() throws Exception {
        int occupiedPort;
        try (ServerSocket occupied = new ServerSocket(0)) {
            occupiedPort = occupied.getLocalPort();

            try (ServerSocketChannel channel = ServerSocketChannel.open()) {
                channel.configureBlocking(false);

                ConnectionHandler handler = new ConnectionHandler(serverContext, false);

                Method method = ConnectionHandler.class.getDeclaredMethod(
                        "bindToNextAvailablePort", int.class, ServerSocketChannel.class);
                method.setAccessible(true);

                int boundPort = (int) method.invoke(handler, occupiedPort, channel);

                assertNotEquals("should skip the occupied port", occupiedPort, boundPort);
                assertEquals("channel should be bound to returned port", boundPort, channel.socket().getLocalPort());
            }
        }
    }

    @Test
    public void addAndRemovePeerInHandshakeUpdatesCollection() throws Exception {
        ConnectionHandler handler = new ConnectionHandler(serverContext, false);

        try (SocketChannel socketChannel = SocketChannel.open()) {
            PeerInHandshake peer = new PeerInHandshake("127.0.0.1", socketChannel);

            int before = ConnectionHandler.peerInHandshakes.size();
            handler.addPeerInHandshake(peer);
            assertTrue(ConnectionHandler.peerInHandshakes.contains(peer));
            assertEquals(before + 1, ConnectionHandler.peerInHandshakes.size());

            handler.removePeerInHandshake(peer);
            assertFalse(ConnectionHandler.peerInHandshakes.contains(peer));
            assertEquals(before, ConnectionHandler.peerInHandshakes.size());
        }
    }

    @Test
    public void addServerSocketChannelRegistersForAccept() throws Exception {
        ConnectionHandler handler = new ConnectionHandler(serverContext, false);

        try (ServerSocketChannel channel = ServerSocketChannel.open()) {
            channel.configureBlocking(false);
            channel.bind(new InetSocketAddress(0));

            handler.addServerSocketChannel(channel);
            channelsToClose.add(channel);

            boolean found = false;
            for (SelectionKey key : ConnectionHandler.selector.keys()) {
                if (key.channel() == channel) {
                    found = true;
                    assertTrue((key.interestOps() & SelectionKey.OP_ACCEPT) != 0);
                    break;
                }
            }
            assertTrue("channel should be registered with selector", found);
        }
    }
}
