package im.redpanda.core;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class PeerInHandshake {

    String ip;
    int port = 0;
    int status = 0;
    KademliaId identity;
    NodeId nodeId;
    Peer peer;
    SocketChannel socketChannel;
    SelectionKey key;

    public PeerInHandshake(String ip, SocketChannel socketChannel) {
        this.ip = ip;
        this.socketChannel = socketChannel;
    }

    public PeerInHandshake(String ip, Peer peer, SocketChannel socketChannel) {
        this.ip = ip;
        this.peer = peer;
        this.socketChannel = socketChannel;
    }

    /**
     * 0 default value, before any handshake was parsed.
     * <p></p>
     * 1 first handshake was parsed, here we are waiting to obtain more information of the peer like the public key
     * to finish the complete handshake.
     * -1 handshake finished from our site, we do not expect more data before switching to encryption.
     * We are waiting for the switching byte to start the encryption.
     *
     * @param status
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * For the status information see the setter method.
     *
     * @return
     */
    public int getStatus() {
        return status;
    }

    public PeerInHandshake(String ip) {
        this.ip = ip;
    }


    public void addConnection() {
        try {
            socketChannel.configureBlocking(false);

            SelectionKey key = null;
            ConnectionHandler.selectorLock.lock();
            try {
                ConnectionHandler.selector.wakeup();
//                if (connectionPending) {
//                    peer.isConnecting = true;
//                    peer.setConnected(false);
                key = socketChannel.register(ConnectionHandler.selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
//                } else {
//                    peer.isConnecting = false;
//                    peer.setConnected(true);
//                    key = socketChannel.register(ConnectionHandler.selector, SelectionKey.OP_READ);
//                }
            } finally {
                ConnectionHandler.selectorLock.unlock();
            }


            key.attach(this);
            this.key = key;

//            peer.setSelectionKey(key);
            ConnectionHandler.selector.wakeup();
            Log.putStd("added con");
        } catch (IOException ex) {
            ex.printStackTrace();
            peer.disconnect("could not init connection....");
            return;
        }
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public KademliaId getIdentity() {
        return identity;
    }

    public void setIdentity(KademliaId nonce) {
        this.identity = nonce;
    }

    public Peer getPeer() {
        return peer;
    }

    public void setPeer(Peer peer) {
        this.peer = peer;
    }

    public SelectionKey getKey() {
        return key;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public void setNodeId(NodeId nodeId) {
        this.nodeId = nodeId;
    }
}
