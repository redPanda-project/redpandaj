/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;

import java.io.IOException;
import java.io.Serializable;
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
public class Peer implements Comparable<Peer>, Serializable {

    public String ip;
    public int port;
    public int connectAble = 0;
    public int retries = 0;
    public long lastBufferModified;
    long lastRetryAfter5 = 0;
    public long lastActionOnConnection = 0;
    int cnt = 0;
    public long connectedSince = 0;
    private NodeId nodeId;
    private KademliaId kademliaId;
    private ArrayList<String> filterAdresses;
    private SocketChannel socketChannel;
    //    public ArrayList<ByteBuffer> readBuffers = new ArrayList<ByteBuffer>();
//    public ArrayList<ByteBuffer> writeBuffers = new ArrayList<ByteBuffer>();
    public ByteBuffer readBuffer;
    public ByteBuffer writeBuffer;
    SelectionKey selectionKey;
    public boolean firstCommandsProceeded;
    private boolean connected = false;
    public boolean isConnecting;
    public long lastPinged = 0;
    public double ping = 0;
    public int requestedMsgs = 0;
    byte[] toEncodeForAuthFromMe;
    byte[] toEncodeForAuthFromHim;
    boolean requestedNewAuth;
    public boolean authed = false;
    public ByteBuffer writeBufferCrypted;
    public ByteBuffer readBufferCrypted;
    public int trustRetries = 0;
    public final ReentrantLock writeBufferLock = new ReentrantLock();
    public Thread connectinThread;
    public int parsedCryptedBytes = 0;
    public long syncMessagesSince = 0;
    public ArrayList<Integer> removedSendMessages = new ArrayList<Integer>();
    public int maxSimultaneousRequests = 1;

    public ArrayList<Integer> myInterestedChannelsCodedInHisIDs = new ArrayList<Integer>(); //for perfomance, so I dont have to look in the database for every message i am introduced.

    public long sendBytes = 0;
    public long receivedBytes = 0;

    public boolean isConnectionInitializedByMe = false;

    private boolean isIntegrated = false;

    public Peer(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

//    public void setNodeId(KademliaId nodeId) {
//
//        if (nodeId == null) {
//            return;
//        }
//
//        Test.peerListLock.lock();
//        try {
//            System.out.println("############################################ new node id: " + nodeId + " old: " + this.nodeId);
//
//            if (this.nodeId == null) {
//                //only add
//                //we have to set the new nodeId in advance!
//                this.nodeId = nodeId;
//                Test.addPeerToBucket(this);
//                return;
//            }
//
//            if (this.nodeId.equals(nodeId)) {
//                //maybe a new instance!
//                this.nodeId = nodeId;
//                return;
//            }
//
//            //if we are here the old id is not null and we have a new id/id changed
//            //first remove the old id from bucket
//            Test.removePeerFromBucket(this);
//            System.out.println("removed peer from buckets: new node id: " + nodeId + " old: " + this.nodeId);
//
//
//            //we have to set the new nodeId in advance!
//            this.nodeId = nodeId;
//            Test.addPeerToBucket(this);
//        } finally {
//            Test.peerListLock.unlock();
//        }
//    }
//
//    public void removeNodeId() {
//
//        if (this.nodeId == null) {
//            return;
//        }
//
//        Test.peerListLock.lock();
//        try {
//            Test.removePeerFromBucket(this);
//        } finally {
//            Test.peerListLock.unlock();
//        }
//    }

    public KademliaId getKademliaId() {
        return kademliaId;
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

            if (kademliaId == null || n2.kademliaId == null) {
                return false;
            }

            //return (ip.equals(n2.ip) && port == n2.port && nonce == n2.nonce);
            return kademliaId.equals(n2.kademliaId);

        } else {
            return false;
        }

    }


    public boolean equalsInstance(Object obj) {
        return super.equals(obj);
    }

//    @Override
//    public boolean equals(Object obj) {
//        throw new RuntimeException("hjgadzagdzwad");
////        return equalsNonce(obj);
//    }

    public long getLastAnswered() {
        return System.currentTimeMillis() - lastActionOnConnection;
    }


//    public PeerSaveable toSaveable() {
//        return new PeerSaveable(ip, port, lastAllMsgsQuerried, nodeId, retries);
//    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    @Override
    public int compareTo(Peer o) {

        return o.getPriority() - getPriority();

//        int ret = (int) (retries - o.retries);
//
//        if (ret != 0) {
//            return ret;
//        }
//
//
//        int a = (int) (o.lastActionOnConnection - lastActionOnConnection);
//
//        if (a != 0) {
//            return a;
//        }
//
//        return (int) (o.lastAllMsgsQuerried - lastAllMsgsQuerried);
    }

    public int getPriority() {

        int a = 0;

        if (connected) {
            a += 2000;
        }

        if (kademliaId == null) {
            a -= 1000;
        }

        if (ip.contains(":")) {
            a += 50;
        }


        a += -retries * 200;

        return a;
    }

    public boolean iSameInstance(Peer p) {
        return super.equals(p);
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }


    public void disconnect(String reason) {

        try {
            writeBufferLock.tryLock(2, TimeUnit.SECONDS);

            Log.put("DISCONNECT: " + reason, 30);

            setConnected(false);


            if (isConnecting && connectinThread != null) {
                connectinThread.interrupt();
            }

            isConnecting = false;
            authed = false;

            if (selectionKey != null) {
                selectionKey.cancel();
            }
            if (socketChannel != null) {
//            ByteBuffer a = ByteBuffer.allocate(1);
//            a.put((byte) 254);
//            a.flip();
//            try {
//                int write = socketChannel.write(a);
//                //System.out.println("QUIT bytes: " + write);
//            } catch (IOException ex) {
//            } catch (NotYetConnectedException e) {
//            }

                if (socketChannel.isOpen()) {
                    try {
                        socketChannel.configureBlocking(false);//ToDo: hack
                        socketChannel.close();
                    } catch (IOException ex) {
                    }
                }
            }

            readBuffer = null;
            readBufferCrypted = null;
            writeBuffer = null;
            writeBufferCrypted = null;

            if (writeBufferLock.isHeldByCurrentThread()) {
                writeBufferLock.unlock();
            }


        } catch (InterruptedException ex) {
            Logger.getLogger(Peer.class.getName()).log(Level.SEVERE, null, ex);
        }

        Server.triggerOutboundthread();

    }


    public void ping() {

        if (getSelectionKey() == null || writeBuffer == null) {
            setConnected(false);
            return;
        }
        if (!getSelectionKey().isValid()) {
            System.out.println("selectionkey invalid11!");
            //disconnect();
            setConnected(false);
            return;
        }

        lastPinged = System.currentTimeMillis();

        if (writeBufferLock.tryLock()) {
            if (writeBuffer.capacity() > 0) {
                writeBuffer.put((byte) 100);
//                System.out.println("pinged...");
            } else {
                System.out.println("didnt ping, buffer has content...");
            }
            writeBufferLock.unlock();
        } else {
            System.out.println("Could not lock for ping!");
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

        boolean remainingBytes;

        if (writeBuffer == null) {
            return false;
        }


        if (getSelectionKey().isValid()) {
            Server.connectionHandler.selectorLock.lock();
            try {
                getSelectionKey().selector().wakeup();
                getSelectionKey().interestOps(getSelectionKey().interestOps() | SelectionKey.OP_WRITE);
            } catch (CancelledKeyException e) {
                System.out.println("cancelled key exception");
            } finally {
                Server.connectionHandler.selectorLock.unlock();
            }
        } else {
            System.out.println("key is not valid");
            disconnect("key is not valid");
        }

        return false;
    }

    int writeBytesToPeer(ByteBuffer writeBuffer) throws IOException {
        int writtenBytes = 0;

        writeBuffer.flip();
        //TODO groesse anpassen vom crypted buffer
        byte[] buffer = new byte[Math.min(writeBuffer.remaining(), writeBufferCrypted.remaining())];
        writeBuffer.get(buffer);
        writeBuffer.compact();
//            byte[] encryptedBytes = writeKey.encrypt(buffer);
//encryption disabled, just copy over the original bytes
        byte[] encryptedBytes = buffer;

//            if (writeBufferCrypted.remaining() < encryptedBytes.length) {
//                //buffer zu klein :(
//                ByteBuffer newBuffer = ByteBuffer.allocate(writeBufferCrypted.capacity() + encryptedBytes.length);
//                newBuffer.put(writeBufferCrypted);
//                writeBufferCrypted = newBuffer;
//            }

        writeBufferCrypted.put(encryptedBytes);
        writeBufferCrypted.flip();
        writtenBytes = getSocketChannel().write(writeBufferCrypted);
        writeBufferCrypted.compact();

//            System.out.println("written bytes to node: " + writtenBytes);
        //System.out.println("crypted bytes: " + Utils.bytesToHexString(buffer) + " to " + Utils.bytesToHexString(encryptedBytes));


        return writtenBytes;
    }


    public boolean peerIsHigher() {
        for (int i = 0; i < KademliaId.ID_LENGTH / 8; i++) {
            int compare = Byte.toUnsignedInt(kademliaId.getBytes()[i]) - Byte.toUnsignedInt(Server.NONCE.getBytes()[i]);
            if (compare > 0) {
                return true;
            } else if (compare < 0) {
                return false;
            }
        }
        System.out.println("could not compare!!!");
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

    public boolean isCryptedConnection() {
        return readBufferCrypted != null;
    }

    public ReentrantLock getWriteBufferLock() {
        return writeBufferLock;
    }

    public ByteBuffer getWriteBuffer() {
        return writeBuffer;
    }

    public boolean isIntegrated() {

        if (isIntegrated) {
            return true;
        }

        if (connectedSince != 0 && System.currentTimeMillis() - connectedSince > 1000L * 10L) {
            isIntegrated = true;
        }

        return false;
    }

    public void setKademliaId(KademliaId kademliaId) {

        if (kademliaId == null) {
            return;
        }

        Server.peerListLock.writeLock().lock();
        try {
            System.out.println("############################################ new node id: " + kademliaId + " old: " + this.kademliaId);

            if (this.kademliaId == null) {
                //only add
                //we have to set the new nodeId in advance!
                this.kademliaId = kademliaId;
                Server.addPeerToBucket(this);
                return;
            }

            if (this.kademliaId.equals(kademliaId)) {
                //maybe a new instance!
                this.kademliaId = kademliaId;
                return;
            }

            //if we are here the old id is not null and we have a new id/id changed
            //first remove the old id from bucket
            Server.removePeerFromBucket(this);
            System.out.println("removed peer from buckets: new node id: " + kademliaId + " old: " + this.kademliaId);


            //we have to set the new nodeId in advance!
            this.kademliaId = kademliaId;
            Server.addPeerToBucket(this);
        } finally {
            Server.peerListLock.writeLock().unlock();
        }
    }

    public PeerSaveable toSaveable() {
        return new PeerSaveable(ip, port, kademliaId, retries);
    }

    public void removeNodeId() {

        if (this.kademliaId == null) {
            return;
        }

        Server.peerListLock.writeLock().lock();
        try {
            Server.removePeerFromBucket(this);
        } finally {
            Server.peerListLock.writeLock().unlock();
        }
    }
}
