/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;

import im.redpanda.core.exceptions.PeerProtocolException;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author rflohr
 */
public class Peer implements Comparable<Peer> {

    private Node node;
    public String ip;
    public int port;
    public int connectAble = 0;

    public boolean lightClient = false;
    public int protocolVersion;

    public int retries = 0;
    @Getter
    @Setter
    private long lastPongReceived = 0;
    int cnt = 0;
    public long connectedSince = 0;
    private NodeId nodeId;
    private ArrayList<String> filterAdresses;
    private SocketChannel socketChannel;
    public ByteBuffer readBuffer;
    public ByteBuffer writeBuffer;
    SelectionKey selectionKey;
    private boolean connected = false;
    public boolean isConnecting;
    public long lastPinged = 0;
    public double ping = 0;
    public boolean authed = false;
    public ByteBuffer writeBufferCrypted;
    public final ReentrantLock writeBufferLock = new ReentrantLock();
    public Thread connectingThread;
    public ArrayList<Integer> removedSendMessages = new ArrayList<>();
    public byte lastCommand;


    public long sendBytes = 0;
    public long receivedBytes = 0;

    public boolean isConnectionInitializedByMe = false;

    private boolean isIntegrated = false;

    //new variables since redpanda2.0
    private PeerChiperStreams peerChiperStreams;

    public Peer(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public Peer(String ip, int port, NodeId id) {
        this.ip = ip;
        this.port = port;
        this.nodeId = id;
    }

    /**
     * Set the nodeId of this Peer, does not check the consitency with the KademliaId.
     *
     * @param nodeId
     */
    public void setNodeId(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public void clearNode() {
        this.node = null;
    }

    public void setNode(Node node) {

        if (this.nodeId != null && !this.nodeId.equals(node.getNodeId())) {
            System.out.println(String.format("set wrong node to peer, panic: %s - %s", this.nodeId, node.getNodeId()));
        }

        this.node = node;
    }

    public Node getNode() {
        if (!isAuthed() || !connected) {
            return null;
        }
        return node;
    }

    public KademliaId getKademliaId() {
        if (getNodeId() == null) {
            return null;
        }
        return getNodeId().getKademliaId();
    }

    public boolean equalsIpAndPort(Object obj) {

        if (obj instanceof Peer) {

            Peer n2 = (Peer) obj;

            return (ip.equals(n2.ip) && port == n2.port);

        } else {
            return false;
        }

    }

    public boolean equalsNonce(Object obj) {

        if (obj instanceof Peer) {

            Peer n2 = (Peer) obj;

            if (getNodeId() == null || getNodeId().getKademliaId() == null || n2.getNodeId() == null || n2.getNodeId().getKademliaId() == null) {
                return false;
            }

            return getNodeId().getKademliaId().equals(n2.getNodeId().getKademliaId());
        } else {
            return false;
        }

    }


    public boolean equalsInstance(Object obj) {
        return super.equals(obj);
    }

    public long getLastAnswered() {
        return System.currentTimeMillis() - lastPongReceived;
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    @Override
    public int compareTo(Peer o) {
        return o.getPriority() - getPriority();
    }

    public int getPriority() {

        int score = 0;

        if (connected) {
            score += 2000;
        }

        if (getNodeId() == null) {
            score -= 1000;
        }

        if (ip != null && ip.contains(":")) {
            score += 50;
        }

        score += -retries * 200;

        if (node != null) {
            score += 5000;

            score -= node.getGmTestsFailed() * 3;
            score += node.getGmTestsSuccessful() * 5;
        }

        return score;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public void disconnect(String reason) {

        clearNode();
        isConnecting = false;
        authed = false;
        connectedSince = 0;
        isIntegrated = false;

        try {
            writeBufferLock.tryLock(2, TimeUnit.SECONDS);

            Log.put("DISCONNECT: " + reason, 100);

            setConnected(false);


            if (isConnecting && connectingThread != null) {
                connectingThread.interrupt();
            }


            if (selectionKey != null) {
                selectionKey.cancel();
            }
            if (socketChannel != null) {
                if (socketChannel.isOpen()) {
                    try {
                        socketChannel.configureBlocking(false);//ToDo: hack
                    } catch (IOException ignored) {
                    }
                }

                try {
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (readBuffer != null) {
                ByteBuffer buff = readBuffer;
                readBuffer = null;
                buff.position(0);
                buff.limit(buff.capacity());
                ByteBufferPool.returnObject(buff);
            }

            writeBuffer = null;
            writeBufferCrypted = null;

            if (writeBufferLock.isHeldByCurrentThread()) {
                writeBufferLock.unlock();
            }

        } catch (InterruptedException ex) {
            Logger.getLogger(Peer.class.getName()).log(Level.SEVERE, null, ex);
            Thread.currentThread().interrupt();
        }

        Server.triggerOutboundThread();

    }


    public void sendPing() {

        if (System.currentTimeMillis() - lastPinged < 1000) {
            return;
        }

        if (getSelectionKey() == null || writeBuffer == null) {
            setConnected(false);
            return;
        }
        if (!getSelectionKey().isValid()) {
            System.out.println("selectionkey invalid11!");
            setConnected(false);
            return;
        }

        lastPinged = System.currentTimeMillis();

        if (writeBufferLock.tryLock()) {
            if (writeBuffer.capacity() > 0) {
                writeBuffer.put(Command.PING);
                Log.put("pinged...", 100);
            } else {
                Log.put("didnt ping, buffer has content...", 100);
            }
            writeBufferLock.unlock();
        } else {
            Log.put("Could not lock for ping!", 50);
        }

        setWriteBufferFilled();

    }

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public boolean setWriteBufferFilled() {

        if (!isConnected()) {
            return false;
        }

        if (writeBuffer == null) {
            return false;
        }


        if (getSelectionKey().isValid()) {
            ConnectionHandler.selectorLock.lock();
            try {
                getSelectionKey().selector().wakeup();
                getSelectionKey().interestOps(getSelectionKey().interestOps() | SelectionKey.OP_WRITE);
                return true;
            } catch (CancelledKeyException e) {
                System.out.println("cancelled key exception");
            } finally {
                ConnectionHandler.selectorLock.unlock();
            }
        } else {
            System.out.println("key is not valid");
            disconnect("key is not valid");
        }

        return false;
    }

    public void encrypteOutputdata() {

        writeBufferLock.lock();
        try {

            if (writeBuffer == null) {
                return;
            }

            writeBuffer.flip();
            int remaining = writeBuffer.remaining();


            if (remaining == 0) {
                writeBuffer.compact();
                return;
            }

            //writebuffer in read, writeBufferCrypted in write mode
            getPeerChiperStreams().encrypt(writeBuffer, writeBufferCrypted);

            writeBuffer.compact();
        } finally {
            writeBufferLock.unlock();
        }
    }

    public int decryptInputData(ByteBuffer byteBufferToDecrypt) throws PeerProtocolException {

        writeBufferLock.lock();
        try {


            byteBufferToDecrypt.flip();
            int remaining = byteBufferToDecrypt.remaining();


            if (remaining == 0) {
                byteBufferToDecrypt.compact();
                return 0;
            }

            if (readBuffer.remaining() < remaining) {
                int newSize = Math.min(2 * readBuffer.position() + 2 * readBuffer.remaining(), 1024 * 1024 * 60);
                if (newSize == readBuffer.remaining()) {
                    throw new PeerProtocolException(String.format("buffer could not be increased, size is %s ", newSize));
                }
                Log.put(String.format("get new readBuffer with size: %s", newSize), 5);
                ByteBuffer newBuffer = ByteBufferPool.borrowObject(newSize);

                System.arraycopy(readBuffer.array(), 0, newBuffer.array(), 0, readBuffer.array().length);
                newBuffer.position(readBuffer.position());
                readBuffer.compact();
                readBuffer.position(0);
                ByteBufferPool.returnObject(readBuffer);
                readBuffer = newBuffer;
            }

            getPeerChiperStreams().decrypt(byteBufferToDecrypt, readBuffer);

            byteBufferToDecrypt.compact();

            return remaining;
        } finally {
            writeBufferLock.unlock();
        }


    }

    int writeBytesToPeer() throws IOException {
        writeBufferCrypted.flip();
        int writtenBytes = getSocketChannel().write(writeBufferCrypted);
        Log.put("written bytes to node: " + writtenBytes + " remaining: " + writeBufferCrypted.remaining(), 100);
        writeBufferCrypted.compact();

        return writtenBytes;
    }


    public boolean peerIsHigher(ServerContext serverContext) {
        for (int i = 0; i < KademliaId.ID_LENGTH / 8; i++) {
            int compare = Byte.toUnsignedInt(getKademliaId().getBytes()[i]) - Byte.toUnsignedInt(serverContext.getNonce().getBytes()[i]);
            if (compare > 0) {
                return true;
            } else if (compare < 0) {
                return false;
            }
        }
        return false;
    }


    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public boolean isAuthed() {
        return authed;
    }

    public ReentrantLock getWriteBufferLock() {
        return writeBufferLock;
    }

    public ByteBuffer getWriteBuffer() {
        return writeBuffer;
    }

    public boolean isIntegrated() {

        if (lightClient) {
            return false;
        }

        if (isIntegrated) {
            return true;
        }

        if (connectedSince != 0 && System.currentTimeMillis() - connectedSince > 1000L * 10L) {
            isIntegrated = true;
        }

        return false;
    }

    public PeerSaveable toSaveable() {
        return new PeerSaveable(ip, port, nodeId, retries);
    }

    public void setLastPongReceived(long lastPongReceived) {
        this.lastPongReceived = lastPongReceived;
    }

    public void setPeerChiperStreams(PeerChiperStreams peerChiperStreams) {
        this.peerChiperStreams = peerChiperStreams;
    }

    public PeerChiperStreams getPeerChiperStreams() {
        return peerChiperStreams;
    }

    public void setupConnectionForPeer(PeerInHandshake peerInHandshake) {
        //disconnect old connection if present
        disconnect("new connection for this peer");

        setConnected(true);
        isConnecting = false;
        authed = true;
        retries = 0;
        lightClient = peerInHandshake.lightClient;
        protocolVersion = peerInHandshake.protocolVersion;
        connectedSince = System.currentTimeMillis();

        /**
         * setup the buffers
         */
        ReentrantLock writeBufferLock = getWriteBufferLock();
        writeBufferLock.lock();
        try {
            writeBuffer = ByteBuffer.allocate(300 * 1024);
            writeBufferCrypted = ByteBuffer.allocate(300 * 1024);
        } catch (Exception e) {
            Log.putStd("Could not reserve enough memory for this connection. Disconnect peer...");
            disconnect("Could not reserve enough memory for this connection.");
        } finally {
            writeBufferLock.unlock();
        }

        //set up the peer with all data from the peerInHandshake
        setLastPongReceived(System.currentTimeMillis());

        setSocketChannel(peerInHandshake.getSocketChannel());
        setSelectionKey(peerInHandshake.getKey());

        setPeerChiperStreams(peerInHandshake.getPeerChiperStreams());

        if (!peerInHandshake.lightClient) {
            writeBufferLock.lock();
            try {
                writeBuffer.put(Command.UPDATE_REQUEST_TIMESTAMP);
                writeBuffer.put(Command.ANDROID_UPDATE_REQUEST_TIMESTAMP);
                //peers will now only be requested by the RequestPeerListJob
                setWriteBufferFilled();
            } finally {
                writeBufferLock.unlock();
            }
        }
    }

    @Override
    public String toString() {
        return "Peer{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                '}';
    }

    public void setLightClient(boolean lightClient) {
        this.lightClient = lightClient;
    }

    public boolean isLightClient() {
        return lightClient;
    }

    /**
     * Do not call this method directly, instead use Peerlist.clearConnectionDetails(Peer peer)
     */
    public void removeIpAndPort() {
        ip = null;
        port = 0;
    }

    public boolean hasNode() {
        return getNode() != null;
    }
}
