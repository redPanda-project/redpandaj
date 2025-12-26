/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;


import im.redpanda.core.exceptions.PeerProtocolException;
import im.redpanda.crypt.Utils;
import im.redpanda.jobs.Job;
import im.redpanda.jobs.KademliaSearchJob;
import im.redpanda.kademlia.KadContent;
import io.sentry.Breadcrumb;
import io.sentry.Sentry;
import io.sentry.SentryLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


/**
 * @author robin
 */
public class ConnectionReaderThread extends Thread {

    private static final Logger logger = LogManager.getLogger();
    public static final String ANDROID_UPDATE_FILE = "android.apk";

    public static final int STD_TIMEOUT = 10;
    public static final int MIN_SIGNATURE_LEN = 70;
    private static final ArrayList<ConnectionReaderThread> threads = new ArrayList<>();
    public static final ReentrantLock threadLock = new ReentrantLock(false);
    public static final ExecutorService threadPool = Executors.newFixedThreadPool(4);


    /**
     * Here we can set the max simultaneously uploads.
     */
    static final Semaphore updateUploadLock = new Semaphore(1);
    static final ReentrantLock updateDownloadLock = new ReentrantLock();
    private final PeerList peerList;
    private final ByteBuffer myReaderBuffer = ByteBuffer.allocate(1024 * 50);
    private final ServerContext serverContext;
    private final InboundCommandProcessor inboundProcessor;

    private boolean run = true;
    private final int timeout;
    private int maxThreads = 30;

    private int peekedAndFound = 0;
    private int lastThreadSize = 1;


    /**
     * Timeout in seconds for polling for new work to do.
     *
     * @param serverContext
     * @param timeout
     */
    public ConnectionReaderThread(ServerContext serverContext, int timeout) {
        this.serverContext = serverContext;
        this.timeout = timeout;
        this.peerList = serverContext.getPeerList();
        this.inboundProcessor = new InboundCommandProcessor(serverContext);
        Log.putStd("########################## spawned new connectionReaderThread!!!!");
        start();

    }

    public static void init(ServerContext serverContext) {
        threadLock.lock();
        threads.add(new ConnectionReaderThread(serverContext, -1));
        threadLock.unlock();
        Log.putStd("wwoooo");

        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> Log.putCritical(throwable));


    }

    public static boolean parseHandshake(ServerContext serverContext, PeerInHandshake peerInHandshake, ByteBuffer buffer) {

        PeerList peerList = serverContext.getPeerList();
        if (buffer.remaining() < 30) {
            System.out.println("not enough bytes for handshake");
            return false;
        }


        String magic = readString(buffer, 4);

        int version = buffer.get();
        peerInHandshake.setProtocolVersion(version);

        int clientType = buffer.get();

        if (clientType > 128 || clientType < 0) {
            peerInHandshake.setLightClient(true);
        }

        byte[] nonceBytes = new byte[KademliaId.ID_LENGTH / 8];
        buffer.get(nonceBytes);

        KademliaId identity = new KademliaId(nonceBytes);

        int port = buffer.getInt();

        peerInHandshake.setIdentity(identity);

        peerInHandshake.setPort(port);

        if (port < 0 || port > 65535) {
            System.out.println("wrong port...");
            return false;
        }

        Log.put("Verbindungsaufbau (" + peerInHandshake.ip + "): " + magic + " " + version + " " + identity + " " + port, 10);

        buffer.compact();

        if (identity.equals(serverContext.getNonce())) {
            /**
             * We connected to ourselves, disconnect
             */
            System.out.println("connected to ourselves, disconnecting...");
            peerInHandshake.setStatus(2); //set disconnect code
            try {
                peerInHandshake.getSocketChannel().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            /**
             * Lets remove this peer from our peerlist if it is present, note that an incoming connection is not in our peerlist
             */
            if (peerInHandshake.getPeer() != null) {
//                PeerList.remove(peerInHandshake.getPeer());
                boolean b = peerList.removeIpPort(peerInHandshake.ip, peerInHandshake.port);
                System.out.println("remove of peer successful?: " + b);
            }
            return false;
        }

        /**
         * If the connection was not initialized by us we have to find the peer first for this handshake.
         */
        if (peerInHandshake.getPeer() == null) {
            Peer peer = peerList.get(identity);
            if (peer == null) {
                //No peer found with this identity, lets create a new Peer instance and add it to the list
                peer = new Peer(peerInHandshake.ip, peerInHandshake.port);
                peer.setNodeId(new NodeId(identity));
            } else {
                //lets transfer the NodeId from Peer to the handshake...
                peerInHandshake.setNodeId(peer.getNodeId());
            }
            peerInHandshake.setPeer(peer);
        } else {
            /**
             * Lets check if the node send us the expected Identity
             */
            if (!identity.equals(peerInHandshake.getPeer().getKademliaId())) {
                // the Identity is not as expected, maybe there where no Identity for this peer?
                if (peerInHandshake.getPeer().getKademliaId() == null) {
                    //we can now update the Identity of the Peer since we had non, most likely we connect from a reseed list
                    peerList.updateKademliaId(peerInHandshake.getPeer(), identity);
                } else {
                    Log.put("wrong identity for that peer, disconnecting....", 30);
                    try {
                        peerInHandshake.getSocketChannel().close();
                        peerInHandshake.getKey().cancel();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    /**
                     * Lets create a new Peer with the connection details but without any Identity so that this Peer
                     * can be used, maybe the client wiped its data.
                     */
                    peerList.clearConnectionDetails(peerInHandshake.getPeer());
                    peerList.add(new Peer(peerInHandshake.ip, peerInHandshake.port));
                    Log.put("we addded that connection details: " + peerInHandshake.ip + ":" + peerInHandshake.port, 30);
                }
            }
        }

        //lets check if the peer has a NodeId
        if (peerInHandshake.getPeer().getNodeId() == null) {
            /**
             * Since the Peer has no NodeId, we have to search the peer in the PeerList or
             * request the public key of the Peer.
             */

            Peer peer = peerList.get(identity);
            if (peer != null) {
                /**
                 * We found a peer for this KademliaId, lets set the data for the PeerInHandShake
                 */
                peerInHandshake.setPeer(peer);

                if (peerInHandshake.getPeer().getNodeId() == null || peerInHandshake.getPeer().getNodeId().keyPair == null) {
                    peerInHandshake.setStatus(1);
                    requestPublicKey(peerInHandshake);
                } else {
                    peerInHandshake.setNodeId(peer.getNodeId());
                    /**
                     * We set the status of the handshake to finished from our site since we are not expecting more data
                     * to complete the handshake, the other peer may still request our public key.
                     */
                    peerInHandshake.setStatus(-1);
                }


            } else {
                /**
                 * We set the status of the handshake that we are still awaiting data from the Peer to complete the handshake
                 */
                requestPublicKey(peerInHandshake);
            }
        } else {

            //lets check if the NodeId has a keypair
            if (peerInHandshake.getPeer().getNodeId().keyPair == null) {
                peerInHandshake.setStatus(1);
                requestPublicKey(peerInHandshake);
            } else {
                /**
                 * We set the status of the handshake to finished from our site since we are not expecting more data
                 * to complete the handshake, the other peer may still request our public key.
                 */
                peerInHandshake.setStatus(-1);
            }
        }

        System.out.println("peer status for handshake: " + peerInHandshake.getStatus());
        return true;
    }


    private int readConnection(Peer peer) throws PeerProtocolException {


        ByteBuffer writeBuffer = peer.writeBuffer;
        SelectionKey key = peer.selectionKey;


//        if (myReaderBuffer.position() != 0) {
//            throw new RuntimeException("buffer has to be at position 0, otherwise we would parse data from a different peer.");
//        }

        int read = -2;
        String debugStringRead = myReaderBuffer.toString();
        try {
            read = peer.getSocketChannel().read(myReaderBuffer);
            Log.put("!!read bytes: " + read, 200);
        } catch (IOException e) {
//            e.printStackTrace();
            key.cancel();
            peer.disconnect("could not read peer...");
            return 0;
        } catch (Throwable e) {
            Log.sentry(e);
            Log.sentry("Could not read in ConnectionReaderThread, buffer before read was: " + debugStringRead);
            e.printStackTrace();
            key.cancel();
            peer.disconnect("could not read...");
            return 0;
        }

        if (read == -2) {
            Log.putStd("hgdjawhgdzawdtgzaud");
        }

        if (read > 0) {
            Server.inBytes += read;
            peer.receivedBytes += read;
        }
        if (read == 0) {
            Log.putStd("dafuq 2332");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Breadcrumb breadcrumb = new Breadcrumb();
            breadcrumb.setCategory("IO");
            breadcrumb.setMessage("myReaderBuffer: " + myReaderBuffer + " current command: " + (myReaderBuffer.remaining() > 0 ? myReaderBuffer.duplicate().get() : "no command"));
            breadcrumb.setLevel(SentryLevel.WARNING);
            Sentry.addBreadcrumb(breadcrumb);
            Log.sentry("read 0 bytes...");
            return 0;
        } else if (read == -1) {
            Log.put("closing connection " + peer.ip + ": not readable! ", 100);
            peer.disconnect(" read == -1 ");
            key.cancel();
        } else {

            Log.put("received bytes!", 200);


        }


        if (peer.readBuffer == null) {
            peer.readBuffer = ByteBufferPool.borrowObject(myReaderBuffer.position());
        }

        ByteBuffer readBuffer = peer.readBuffer;


        /**
         * Decrypt all bytes from the readBufferCrypted to the readBuffer
         */
        peer.decryptInputData(myReaderBuffer);

        inboundProcessor.loopCommands(peer, readBuffer);

//        System.out.println("buffer after parse: " + readBuffer);

        /**
         * The readBuffer might be null if the peer is disconnected while parsing a command, the disconnect method handles the
         * return of the readBuffer...
         */
        if (peer.readBuffer != null && peer.readBuffer.position() == 0) {
            ByteBufferPool.returnObject(peer.readBuffer);
            peer.readBuffer = null;
        }

        if (myReaderBuffer.position() != 0 && myReaderBuffer.limit() != myReaderBuffer.capacity()) {
            throw new RuntimeException("myReaderBuffer was not ready for the next read: " + myReaderBuffer);
        }

        return read;
    }

    public static void sendHandshake(ServerContext serverContext, PeerInHandshake peerInHandshake) {

        ByteBuffer writeBuffer = ByteBufferPool.borrowObject(30);
        String bufferBeforeWriting = writeBuffer.toString();


        try {
            writeBuffer.put(Server.MAGIC.getBytes());
            writeBuffer.put((byte) Server.VERSION);
            writeBuffer.put((byte) 0); //we are no light client
            writeBuffer.put(serverContext.getNonce().getBytes());
            writeBuffer.putInt(serverContext.getPort());
        } catch (BufferOverflowException e) {
            Log.sentry("bufferoverflow in put magic, buffer before: " + bufferBeforeWriting);
        }

        writeBuffer.flip();

        try {
            int write = peerInHandshake.getSocketChannel().write(writeBuffer);
//            System.out.println("written bytes of handshake: " + write);
            if (write != 30) {
                throw new RuntimeException("could not write all data for handshake...");
            }
        } catch (IOException e) {
            e.printStackTrace();
            try {
                peerInHandshake.getSocketChannel().close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }


        writeBuffer.compact();
        ByteBufferPool.returnObject(writeBuffer);
    }

    public static void sendPublicKeyToPeer(ServerContext serverContext, PeerInHandshake peerInHandshake) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 65);

        buffer.put(Command.SEND_PUBLIC_KEY);
        buffer.put(serverContext.getNodeId().exportPublic());
        buffer.flip();

        try {
            long write = peerInHandshake.getSocketChannel().write(buffer);
            System.out.println("written bytes to SEND_PUBLIC_KEY: " + write);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loopCommands(Peer peer, ByteBuffer readBuffer) {
        inboundProcessor.loopCommands(peer, readBuffer);
    }

    private static void requestPublicKey(PeerInHandshake peerInHandshake) {
        peerInHandshake.setStatus(1);
        ByteBuffer writeBuffer = ByteBuffer.allocate(1);

        writeBuffer.put(Command.REQUEST_PUBLIC_KEY);
        writeBuffer.flip();

        try {
            int write = peerInHandshake.getSocketChannel().write(writeBuffer);
            System.out.println("written bytes to REQUEST_PUBLIC_KEY: " + write);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int parseCommand(byte command, ByteBuffer readBuffer, Peer peer) {
        return inboundProcessor.parseCommand(command, readBuffer, peer);
    }
    private int parseKademliaGetAnswer(ByteBuffer readBuffer, Peer peer) {
        if (4 + 8 + NodeId.PUBLIC_KEYLEN + 4 + MIN_SIGNATURE_LEN > readBuffer.remaining()) {
            return 0;
        }

        int ackId = readBuffer.getInt();

        long timestamp = readBuffer.getLong();

        byte[] publicKeyBytes = new byte[NodeId.PUBLIC_KEYLEN];
        readBuffer.get(publicKeyBytes);

        int contentLen = readBuffer.getInt();

        if (contentLen > readBuffer.remaining()) {
            return 0;
        }

        if (contentLen < 0 && contentLen > 1024 * 1024 * 10) {
            peer.disconnect("wrong contentLen for kadcontent");
            return 0;
        }

        byte[] contentBytes = new byte[contentLen];
        readBuffer.get(contentBytes);

        if (MIN_SIGNATURE_LEN > readBuffer.remaining()) {
            return 0;
        }

        byte[] signatureBytes = Utils.readSignature(readBuffer);
        if (signatureBytes == null) {
            return 0;
        }
        int lenOfSignature = signatureBytes.length;

        KadContent kadContent = new KadContent(timestamp, publicKeyBytes, contentBytes, signatureBytes);

        if (kadContent.verify()) {
            boolean saved = serverContext.getKadStoreManager().put(kadContent);

            KademliaSearchJob runningJob = (KademliaSearchJob) Job.getRunningJob(ackId);
            if (runningJob != null) {
                runningJob.ack(kadContent, peer);
            }

        } else {
            //todo
            System.out.println("kadContent verification failed!!!");
        }


        return 1 + 4 + 8 + NodeId.PUBLIC_KEYLEN + 4 + contentLen + lenOfSignature;
    }

    @Override
    public void run() {

        setName("ReaderThread");


        while (!Server.shuttingDown && run) {


            if (killThreadIfMaxThreadsReached()) {
                continue;
            }

            Peer peer = null;
            try {

                if (timeout == -1) {
                    peer = ConnectionHandler.peersToReadAndParse.take(); // will never return null element, needed for main thread. Dann ist immer einer am Leben.
                } else {
                    peer = ConnectionHandler.peersToReadAndParse.poll(timeout, TimeUnit.SECONDS);
                }

                int size = ConnectionHandler.peersToReadAndParse.size();
                if (size > 20) {
                    System.out.println("too many peers waiting for read: " + size);
                }

                if (ConnectionHandler.peersToReadAndParse.peek() != null) {

                    if (peekedAndFound < 0) {
                        peekedAndFound = 0;
                    }


                    peekedAndFound++;

                    if (peekedAndFound > 5) {

                        threadLock.lock();
                        if (threads.size() < maxThreads) {

                            try {
                                ConnectionReaderThread connectionReaderThread = new ConnectionReaderThread(serverContext, STD_TIMEOUT);
                                threads.add(connectionReaderThread);
                                Log.put("threads now: " + threads.size(), -10);
                            } catch (Throwable e) {
                                maxThreads = maxThreads - 1;
                                System.out.println("reducing max threads: " + maxThreads);
                            }

                        }
                        threadLock.unlock();

                        if (peekedAndFound > 0) {
                            peekedAndFound = 0;
                        }

                    }
                } else {
                    peekedAndFound--;
                    if (peekedAndFound > 0) {
                        peekedAndFound = 0;
                    }
                    if (peekedAndFound < -5) {
                        peekedAndFound = -5;
                    }

                    if (timeout != -1 && peekedAndFound < -5) {
                        threadLock.lock();
                        run = false;
                        threads.remove(this);
                        threadLock.unlock();
                        Log.put("last time this thead will run, threads afterwards: " + threads.size(), -10);
                    }

                }

            } catch (InterruptedException ex) {
                Log.putStd("interrupted, finish thread...");
                run = false;
                continue;
            } catch (Throwable e) {
                e.printStackTrace();
            }


            if (peer == null) {
                //this thread can be destroyed, a new one will be started if needed
                run = false;
                threadLock.lock();
                threads.remove(this);
                Log.put("threads now: " + threads.size(), -10);
                threadLock.unlock();
                continue;
            }

            long a = System.currentTimeMillis();

            try {
                readConnection(peer);
            } catch (PeerProtocolException e) {
                Log.sentry(e);
                peer.disconnect("PeerProtocolException");
            } catch (Throwable e) {
                Log.sentry(e);
            }


            long diff = (System.currentTimeMillis() - a);

            if (diff > 5000L) {
                Log.sentry("command took over 5 seconds to parse: %s".formatted(diff));
            }

            ConnectionHandler.doneRead.add(peer);

            ConnectionHandler.selector.wakeup();

        }


    }


    private boolean killThreadIfMaxThreadsReached() {
        int threadSize = 1;
        threadLock.lock();
        try {
            threadSize = threads.size();
            if (threadSize > maxThreads) {
                run = false;
                threads.remove(this);
                Log.put("threads now: " + threads.size(), -10);
                return true;
            }
        } finally {
            threadLock.unlock();
        }

        if (threadSize != lastThreadSize) {
            peekedAndFound = 0;
        }
        lastThreadSize = threads.size();
        return false;
    }


    public static String readString(ByteBuffer byteBuffer, int length) {

        if (byteBuffer.limit() - byteBuffer.arrayOffset() < length) {
            return null; //not enough bytes rdy!
        }
        byteBuffer.position(byteBuffer.position() + length);
        return new String(byteBuffer.array(), byteBuffer.arrayOffset(), length);
    }

    public static String parseString(ByteBuffer byteBuffer) {
        int stringByteLength = byteBuffer.getInt();

        ByteBuffer stringBuffer = ByteBufferPool.borrowObject(stringByteLength);
        try {
            byteBuffer.get(stringBuffer.array(), 0, stringByteLength);
            return new String(stringBuffer.array(), 0, stringByteLength);
        } finally {
            ByteBufferPool.returnObject(stringBuffer);
        }
    }

    public static KademliaId parseKademliaId(ByteBuffer byteBuffer) {
        return KademliaId.fromBuffer(byteBuffer);
    }

}
