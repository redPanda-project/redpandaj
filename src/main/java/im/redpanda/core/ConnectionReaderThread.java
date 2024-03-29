/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;


import im.redpanda.core.exceptions.PeerProtocolException;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.Utils;
import im.redpanda.flaschenpost.GMContent;
import im.redpanda.flaschenpost.GMParser;
import im.redpanda.jobs.Job;
import im.redpanda.jobs.KademliaInsertJob;
import im.redpanda.jobs.KademliaSearchJob;
import im.redpanda.jobs.KademliaSearchJobAnswerPeer;
import im.redpanda.kademlia.KadContent;
import io.sentry.Breadcrumb;
import io.sentry.Sentry;
import io.sentry.SentryLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
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
    private static final Semaphore updateUploadLock = new Semaphore(1);
    private static final ReentrantLock updateDownloadLock = new ReentrantLock();
    private final PeerList peerList;
    private final ByteBuffer myReaderBuffer = ByteBuffer.allocate(1024 * 50);
    private final ServerContext serverContext;

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
        Log.putStd("########################## spawned new connectionReaderThread!!!!");
        start();

    }

    public static void init(ServerContext serverContext) {
        threadLock.lock();
        threads.add(new ConnectionReaderThread(serverContext, -1));
        threadLock.unlock();
        Log.putStd("wwoooo");

        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            public void uncaughtException(Thread thread, Throwable thrwbl) {

                Log.putCritical(thrwbl);

            }
        });


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


        loopCommands(peer, readBuffer);

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
        readBuffer.flip();

        int parsedBytesLocally = -1;

        while (readBuffer.hasRemaining() && parsedBytesLocally != 0 && peer.isConnected()) {

            int newPosition = readBuffer.position(); // lets save the position before touching the buffer


            byte b = readBuffer.get();
            Log.put("command: " + b + " " + readBuffer, 200);


            parsedBytesLocally = parseCommand(b, readBuffer, peer);
            if (!peer.isConnected()) {
                /**
                 * the readBuffer was already returned to the pool by the disconnect method and we are not allowed
                 * to use the readBuffer anymore
                 */
                return;
            }
            peer.lastCommand = b;
            newPosition += parsedBytesLocally;
            readBuffer.position(newPosition);
        }

        readBuffer.compact();
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

        Log.put("cmd: " + command + " " + Command.PING + " " + (command == Command.PING), 200);

        if (command == Command.PING) {
            Log.put("Received ping command", 200);

            if (!serverContext.getPeerList().contains(peer.getKademliaId())) {
                logger.error(String.format("Got PING from node not in our peerlist, lets add it.... %s, id: %s", peer, peer.getKademliaId()));
                serverContext.getPeerList().add(peer);
//                peer.disconnect("node not in peerlist!");
                return 0;
            }

            peer.getWriteBufferLock().lock();
            try {
                peer.writeBuffer.put(Command.PONG);
            } finally {
                peer.getWriteBufferLock().unlock();
            }
            return 1;
        }
        if (command == Command.PONG) {
            Log.put("Received pong command", 200);
            peer.ping = (1 * peer.ping + (double) (System.currentTimeMillis() - peer.lastPinged)) / 2;
            peer.setLastPongReceived(System.currentTimeMillis());

            return 1;
        } else if (command == Command.REQUEST_PEERLIST) {


            peerList.getReadWriteLock().readLock().lock();
            try {

                int size = 0;
                for (Peer peerToWrite : peerList.getPeerArrayList()) {

                    if (peerToWrite.ip == null || peerToWrite.isLightClient() || peerToWrite.getNodeId() == null || !peerToWrite.getNodeId().hasKey()) {
                        continue;
                    }
                    size++;
                }


                int[] peers = new int[size];
                int[] kademliaIds = new int[size];
                int[] ips = new int[size];


                ByteBuffer byteBuffer = ByteBufferPool.borrowObject(1024 * 200);


                byteBuffer.putInt(size);

                try {
                    int cnt = 0;
                    for (Peer peerToWrite : peerList.getPeerArrayList()) {

                        if (peerToWrite.ip == null || peerToWrite.isLightClient() || peerToWrite.getNodeId() == null || !peerToWrite.getNodeId().hasKey()) {
                            continue;
                        }


                        if (peerToWrite.getNodeId() != null && peerToWrite.getNodeId().hasKey()) {
                            byteBuffer.putShort((short) 1);
                            byteBuffer.put(peerToWrite.getNodeId().exportPublic());
                        } else {
                            byteBuffer.putShort((short) 0);
                        }


                        byte[] ipStringBytes = peerToWrite.ip.getBytes();
                        byteBuffer.putInt(ipStringBytes.length);
                        byteBuffer.put(ipStringBytes);

                        byteBuffer.putInt(peerToWrite.getPort());
                        cnt++;
                    }

                    byteBuffer.flip();

                    peer.getWriteBufferLock().lock();
                    try {
                        peer.writeBuffer.put(Command.SEND_PEERLIST);
                        peer.writeBuffer.putInt(byteBuffer.remaining());
                        peer.writeBuffer.put(byteBuffer);
                        peer.setWriteBufferFilled();
                        byteBuffer.compact();
                    } finally {
                        peer.getWriteBufferLock().unlock();
                    }
                } finally {
                    ByteBufferPool.returnObject(byteBuffer);
                }

            } finally {
                peerList.getReadWriteLock().readLock().unlock();
            }

            return 1;
        } else if (command == Command.SEND_PEERLIST) {

            int toRead = readBuffer.getInt();
            if (readBuffer.remaining() < toRead) {
                return 0;
            }

            byte[] bytesForPeerList = new byte[toRead];
            readBuffer.get(bytesForPeerList);


            ByteBuffer peerListBytes = ByteBuffer.wrap(bytesForPeerList);

            int peerListSize = peerListBytes.getInt();

            Peer newPeer;

            for (int i = 0; i < peerListSize; i++) {


                NodeId nodeId = null;
                int booleanNodeIdPresent = peerListBytes.getShort();
                if (booleanNodeIdPresent == 1) {
                    byte[] bytes = new byte[NodeId.PUBLIC_KEYLEN];
                    peerListBytes.get(bytes);
                    nodeId = NodeId.importPublic(bytes);
                }
                String ip = parseString(peerListBytes);
                int port = peerListBytes.getInt();

                if (nodeId != null) {
                    if (nodeId.getKademliaId().equals(serverContext.getNonce())) {
                        Log.put("found ourselves in the peerlist", 80);
                        continue;
                    }

                    if (ip == null) {
                        System.out.println("found a peer with ip null...");
                        continue;
                    }

                    newPeer = new Peer(ip, port, nodeId);


                    Node byKademliaId = Node.getByKademliaId(serverContext, nodeId.getKademliaId());
                    if (byKademliaId != null) {
                        byKademliaId.addConnectionPoint(ip, port);
                    } else {
                        //this will store the new node in the NodeStore as well
                        new Node(serverContext, nodeId);
                    }

                } else {
                    newPeer = new Peer(ip, port);
                }

                Peer add = peerList.add(newPeer);
                if (add == null) {
                    Log.put("new peer added: " + newPeer, 50);
                } else {
                    Log.put("peer was already in peerlist, added new ConnectionPoint: " + newPeer, 50);
                }

            }


            return 1 + 4 + toRead;
        } else if (command == Command.UPDATE_REQUEST_TIMESTAMP) {
            ByteBuffer writeBuffer = peer.getWriteBuffer();
            peer.writeBufferLock.lock();
            try {
                writeBuffer.put(Command.UPDATE_ANSWER_TIMESTAMP);
                writeBuffer.putLong(serverContext.getLocalSettings().getUpdateTimestamp());
            } finally {
                peer.writeBufferLock.unlock();
            }
            peer.setWriteBufferFilled();
            return 1;
        } else if (command == Command.UPDATE_ANSWER_TIMESTAMP) {

            if (8 > readBuffer.remaining()) {
                return 0;
            }

            long othersTimestamp = readBuffer.getLong();

            if (othersTimestamp < serverContext.getLocalSettings().getUpdateTimestamp()) {
                System.out.println("WARNING: peer has outdated redPandaj version! " + peer.getNodeId());
            }

            if (othersTimestamp > serverContext.getLocalSettings().getUpdateTimestamp() && Settings.isLoadUpdates()) {
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        updateDownloadLock.lock();
                        try {
                            System.out.println("our version is outdated, we try to download it from this peer!");
                            peer.writeBufferLock.lock();
                            peer.getWriteBuffer().put(Command.UPDATE_REQUEST_CONTENT);
                            peer.writeBufferLock.unlock();
                            peer.setWriteBufferFilled();


                            //lets not download another version in the next x seconds, otherwise our RAM may explode!
                            try {
                                Thread.sleep(60000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } finally {
                            System.out.println("we can now download it from another peer...");
                            updateDownloadLock.unlock();
                        }

                    }
                };

                Server.threadPool.submit(runnable);
            }


            return 1 + 8;
        } else if (command == Command.UPDATE_REQUEST_CONTENT) {

            if (serverContext.getLocalSettings().getUpdateTimestamp() == -1) {
                return 1;
            }

            if (serverContext.getLocalSettings().getUpdateSignature() == null) {
                System.out.println("we dont have an official signature to upload that update to other peers!");
                return 1;
            }


            Runnable runnable = new Runnable() {
                @Override
                public void run() {

                    updateUploadLock.acquireUninterruptibly();

                    try {

                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        Path path;
                        if (Settings.isSeedNode()) {
                            path = Paths.get("target/redpanda.jar");
                        } else {
                            path = Paths.get("redpanda.jar");
                        }

                        try {
                            System.out.println("we send the update to a peer!");
                            byte[] data = Files.readAllBytes(path);

                            System.out.println("hash data: " + Sha256Hash.create(data));

                            System.out.println("timestamp: " + serverContext.getLocalSettings().getUpdateTimestamp());

                            ByteBuffer a = ByteBuffer.allocate(1 + 8 + 4 + serverContext.getLocalSettings().getUpdateSignature().length + data.length);
                            a.put(Command.UPDATE_ANSWER_CONTENT);
                            a.putLong(serverContext.getLocalSettings().getUpdateTimestamp());
                            a.putInt(data.length);
                            a.put(serverContext.getLocalSettings().getUpdateSignature());
                            a.put(data);
                            if (a.remaining() != 0) {
                                throw new RuntimeException("not enough bytes for the update!!!");
                            }
                            a.flip();

                            peer.writeBufferLock.lock();


                            try {

//                            int pos = 0, toSend = 0;
//
//
//                            byte[] array = a.array();
//
//                            System.out.println("length: " + array.length);
//
//
//                            while (array.length - pos > 0) {
//                                System.out.println("pos: " + pos);
//
//                                toSend = Math.min(writeBuffer.remaining(), array.length - pos);
//
//                                System.out.println("writebuffer r: " + writeBuffer.remaining());
//
//                                System.out.println("toSend: " + toSend);
//
//                                writeBuffer.put(array, pos, toSend);
//
//                                pos += toSend;
//
//                                peer.setWriteBufferFilled();
//                                try {
//                                    Thread.sleep(200);
//                                } catch (InterruptedException e) {
//                                    e.printStackTrace();
//                                }
//                            }


                                if (peer.writeBuffer.remaining() < a.remaining()) {
                                    ByteBuffer allocate = ByteBuffer.allocate(peer.writeBuffer.capacity() + a.remaining() + 1024 * 1024 * 10);
                                    peer.writeBuffer.flip();
                                    allocate.put(peer.writeBuffer);
                                    peer.writeBuffer = allocate;
                                }


//                            System.out.println("" + writeBuffer);

//                            System.out.println("writing bytes: " + a.remaining());

                                peer.writeBuffer.put(a.array());
                                peer.setWriteBufferFilled();

                            } finally {
                                peer.writeBufferLock.unlock();
                            }


                            // only one upload at a time
                            int cnt = 0;
                            while (cnt < 60) {
                                cnt++;
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }

                                peer.writeBufferLock.lock();
                                try {
                                    if (!peer.isConnected() || (peer.writeBuffer.position() == 0 && peer.writeBufferCrypted.position() == 0)) {
                                        break;
                                    }
                                } finally {
                                    peer.writeBufferLock.unlock();
                                }

//                            System.out.println("peer still downloading...");

                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } finally {
                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        updateUploadLock.release();
                    }


                }
            };

            Server.threadPool.submit(runnable);

            return 1;
        } else if (command == Command.UPDATE_ANSWER_CONTENT) {

//            System.out.println("we get a update from node!");

            if (8 + 4 + 72 > readBuffer.remaining()) {
                return 0;
            }


//            System.out.println("we get a update from node 2");
            long othersTimestamp = readBuffer.getLong();
            int toReadBytes = readBuffer.getInt();

            byte[] signature = Utils.readSignature(readBuffer);
            if (signature == null) {
                return 0;
            }
            int lenOfSignature = signature.length;

            if (toReadBytes > readBuffer.remaining()) {
                if (Math.random() < 0.01) {
                    System.out.println("update progress: " + (int) ((double) readBuffer.remaining() / (double) toReadBytes * 100.) + " %");
                }
                return 0;
            }

            System.out.println("update completely in buffer!");

            byte[] data = new byte[toReadBytes];
            readBuffer.get(data);


            if (othersTimestamp > serverContext.getLocalSettings().getUpdateTimestamp() && !Settings.isSeedNode()) {
                System.out.println("we got the update successfully, install it! timestamp: " + othersTimestamp);

                logger.debug("obtained redpandaj update successfully");


                //lets check the signature chunk:
                NodeId nodeId = Updater.getPublicUpdaterKey();


                ByteBuffer bytesToHash = ByteBuffer.allocate(8 + toReadBytes);

                bytesToHash.putLong(othersTimestamp);
                bytesToHash.put(data);

                boolean verified = nodeId.verify(bytesToHash.array(), signature);


                logger.debug("update verified: " + verified);

                File file = new File("redpanda.jar");
                long myCurrentVersionTimestamp = serverContext.getLocalSettings().getUpdateTimestamp();
                if (!file.exists()) {
                    logger.debug("No jar to update found, exiting auto update!");
                    return 1 + 8 + 4 + lenOfSignature + data.length;
                }

                if (myCurrentVersionTimestamp >= othersTimestamp) {
                    logger.debug("update not required our file is newer or equal, aborting...");
                    return 1 + 8 + 4 + lenOfSignature + data.length;
                }

                if (serverContext.getLocalSettings().getUpdateTimestamp() >= othersTimestamp) {
                    logger.debug("update not required our update timestamp is newer or equal, aborting...");
                    return 1 + 8 + 4 + lenOfSignature + data.length;
                }


                if (verified) {

                    try (FileOutputStream fos = new FileOutputStream("update")) {
                        fos.write(data);
                        logger.debug("update store in update file");

                        File f = new File("update");
                        f.setLastModified(othersTimestamp);

                        serverContext.getLocalSettings().setUpdateSignature(signature);
                        serverContext.getLocalSettings().setUpdateTimestamp(othersTimestamp);
                        serverContext.getLocalSettings().save(serverContext.getPort());

                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.exit(0);
                    } catch (FileNotFoundException e) {
                        Log.sentry(e);
                        e.printStackTrace();
                    } catch (IOException e) {
                        Log.sentry(e);
                        Log.sentry(e);
                    }

                }


//                ECKey.ECDSASignature sign = updateChannel.getKey().sign(hash);
//
//                byte[] encodeToDER = new byte[RawMsg.SIGNATURE_LENGRTH];
//                byte[] sigBytes = sign.encodeToDER();
//                System.arraycopy(sigBytes, 0, encodeToDER, 0, sigBytes.length);
//
//                byte[] newBytes = new byte[encodeToDER.length];
//
//                int index = encodeToDER.length - 1;
//                while (true) {
//                    System.arraycopy(encodeToDER, 0, newBytes, 0, index + 1);
//                    if (newBytes[index] == (byte) 0) {
//                        newBytes = new byte[index];
//
//                        index--;
//                    } else {
//                        break;
//                    }
//
//                }
//
//                //System.out.println("Sigbytes len: " + sigBytes.length + " " + Utils.bytesToHexString(encodeToDER));
//                //System.out.println("Sigbytes len: " + sigBytes.length + " " + Utils.bytesToHexString(newBytes));
//                byte[] signature = encodeToDER;
//
//                System.out.println("signature: " + Utils.bytesToHexString(signature));


            }


            return 1 + 8 + 4 + lenOfSignature + data.length;

        } else if (command == Command.ANDROID_UPDATE_REQUEST_TIMESTAMP) {
            File file = new File(ANDROID_UPDATE_FILE);
            if (!file.exists()) {
                return 1;
            }
            peer.writeBufferLock.lock();
            peer.getWriteBuffer().put(Command.ANDROID_UPDATE_ANSWER_TIMESTAMP);
            peer.getWriteBuffer().putLong(serverContext.getLocalSettings().getUpdateAndroidTimestamp());
            peer.writeBufferLock.unlock();
            peer.setWriteBufferFilled();
            return 1;
        } else if (command == Command.ANDROID_UPDATE_ANSWER_TIMESTAMP) {


            if (8 > readBuffer.remaining()) {
                return 0;
            }

            long othersTimestamp = readBuffer.getLong();

            Log.put("Update found from: " + new Date(othersTimestamp) + " our version is from: " + new Date(serverContext.getLocalSettings().getUpdateAndroidTimestamp()), 70);

            if (othersTimestamp < serverContext.getLocalSettings().getUpdateAndroidTimestamp()) {
                System.out.println("WARNING: peer has outdated android.apk version! " + peer.getNodeId());
            }

            /**
             * We can use the exact same timestamp since we now store the timestamp in the local settings
             * and do not count on the reported timestamp of the system.
             */
            if (othersTimestamp > serverContext.getLocalSettings().getUpdateAndroidTimestamp()) {
                Runnable runnable = () -> {
                    updateUploadLock.acquireUninterruptibly();
                    try {

                        if (othersTimestamp <= serverContext.getLocalSettings().getUpdateAndroidTimestamp()) {
                            //maybe we downloaded the update while waiting for lock!
                            System.out.println("already downloaded, skipping...");
                            return;
                        }

                        System.out.println("our android.apk version is outdated, we try to download it from this peer!");
                        peer.writeBufferLock.lock();
                        peer.writeBuffer.put(Command.ANDROID_UPDATE_REQUEST_CONTENT);
                        peer.writeBufferLock.unlock();
                        peer.setWriteBufferFilled();


                        //lets not download another version in the next x seconds, otherwise our RAM may explode!
                        try {
                            Thread.sleep(60000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } finally {
                        System.out.println("we can now download it from another peer...");
                        updateUploadLock.release();
                    }

                };

                threadPool.submit(runnable);
            }


            return 1 + 8;
        } else if (command == Command.ANDROID_UPDATE_REQUEST_CONTENT) {


            if (serverContext.getLocalSettings().getUpdateAndroidSignature() == null) {
                System.out.println("we dont have an official signature to upload that android.apk update to other peers!");
                return 1;
            }


            Runnable runnable = () -> {

                updateUploadLock.acquireUninterruptibly();

                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                Path path = Paths.get(ANDROID_UPDATE_FILE);


                try {
                    System.out.println("we send the android.apk update to a peer!");
                    byte[] data = Files.readAllBytes(path);


                    //lets first check our signature!
                    NodeId publicUpdaterKey = Updater.getPublicUpdaterKey();


                    ByteBuffer bytesToHash = ByteBuffer.allocate(8 + data.length);

                    bytesToHash.putLong(serverContext.getLocalSettings().getUpdateAndroidTimestamp());
                    bytesToHash.put(data);


                    System.out.println("timestamp: " + serverContext.getLocalSettings().getUpdateAndroidTimestamp());

                    System.out.println("signature: " + Utils.bytesToHexString(serverContext.getLocalSettings().getUpdateAndroidSignature()));

                    System.out.println("ver: " + Updater.getPublicUpdaterKey().verify(bytesToHash.array(), serverContext.getLocalSettings().getUpdateAndroidSignature()));

                    boolean verify = publicUpdaterKey.verify(bytesToHash.array(), serverContext.getLocalSettings().getUpdateAndroidSignature());
                    System.out.println("update verified: " + verify);


                    if (!verify) {
                        System.out.println("################################ update not verified " + serverContext.getLocalSettings().getUpdateAndroidTimestamp());
                        return;
                    }

                    byte[] androidSignature = serverContext.getLocalSettings().getUpdateAndroidSignature();

                    ByteBuffer a = ByteBuffer.allocate(1 + 8 + 4 + androidSignature.length + data.length);
                    a.put(Command.ANDROID_UPDATE_ANSWER_CONTENT);
                    a.putLong(serverContext.getLocalSettings().getUpdateAndroidTimestamp());
                    a.putInt(data.length);
                    a.put(androidSignature);
                    a.put(data);
                    a.flip();

                    peer.writeBufferLock.lock();
                    try {
                        if (peer.writeBuffer.remaining() < a.remaining()) {
                            ByteBuffer allocate = ByteBuffer.allocate(peer.writeBuffer.capacity() + a.remaining() + 1024 * 1024 * 10);
                            peer.writeBuffer.flip();
                            allocate.put(peer.writeBuffer);
                            peer.writeBuffer = allocate;
                        }

                        peer.writeBuffer.put(a.array());
                        peer.setWriteBufferFilled();
                    } finally {
                        peer.writeBufferLock.unlock();
                    }


                    // we check every 10 seconds if the upload is already finished
                    int cnt = 0;
                    while (cnt < 6) {
                        cnt++;
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        peer.writeBufferLock.lock();
                        try {
                            if (!peer.isConnected() || (peer.writeBuffer.position() == 0 && peer.writeBufferCrypted.position() == 0)) {
                                break;
                            }
                        } finally {
                            peer.writeBufferLock.unlock();
                        }
                        System.out.println("peer still downloading...");
                    }


                    updateUploadLock.release();


                } catch (IOException e) {
                    e.printStackTrace();
                }
            };

            threadPool.submit(runnable);

            return 1;
        } else if (command == Command.ANDROID_UPDATE_ANSWER_CONTENT) {

            if (8 + 4 + 65 > readBuffer.remaining()) {
                return 0;
            }


            long othersTimestamp = readBuffer.getLong();
            int toReadBytes = readBuffer.getInt();


            byte[] signature = Utils.readSignature(readBuffer);
            if (signature == null) {
                return 0;
            }
            int signatureLen = signature.length;


            if (toReadBytes > readBuffer.remaining()) {
                if (Math.random() < 0.01) {
                    System.out.println("android.apk update progress: " + (int) ((double) readBuffer.remaining() / (double) toReadBytes * 100.) + " %");
                }
                return 0;
            }
            System.out.println("update completely in buffer!");

            byte[] data = new byte[toReadBytes];
            readBuffer.get(data);


            if (othersTimestamp > serverContext.getLocalSettings().getUpdateAndroidTimestamp()) {
                System.out.println("we got the update successfully, lets copy it to hard drive if signature is correct");


                System.out.println("signature found: " + Utils.bytesToHexString(signature));

                //lets check the signature chunk:
                NodeId publicUpdaterKey = Updater.getPublicUpdaterKey();


                ByteBuffer bytesToHash = ByteBuffer.allocate(8 + toReadBytes);

                bytesToHash.putLong(othersTimestamp);
                bytesToHash.put(data);


                boolean verify = publicUpdaterKey.verify(bytesToHash.array(), signature);

                System.out.println("update verified: " + verify);

                if (verify) {

                    try (FileOutputStream fos = new FileOutputStream("android.apk")) {
                        fos.write(data);
                        System.out.println("update stored in android.apk file");

                        File f = new File("android.apk");
                        f.setLastModified(othersTimestamp);

                        serverContext.getLocalSettings().setUpdateAndroidTimestamp(othersTimestamp);
                        serverContext.getLocalSettings().setUpdateAndroidSignature(signature);
                        serverContext.getLocalSettings().save(serverContext.getPort());


                        peerList.getReadWriteLock().readLock().lock();
                        try {
                            for (Peer p : peerList.getPeerArrayList()) {
                                if (!p.isConnected()) {
                                    continue;
                                }
                                p.writeBufferLock.lock();
                                try {
                                    p.writeBuffer.put(Command.ANDROID_UPDATE_ANSWER_TIMESTAMP);
                                    p.writeBuffer.putLong(serverContext.getLocalSettings().getUpdateAndroidTimestamp());
                                } finally {
                                    p.writeBufferLock.unlock();
                                }
                                p.setWriteBufferFilled();
                            }
                        } finally {
                            peerList.getReadWriteLock().readLock().unlock();
                        }


                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }


            }


            return 1 + 8 + 4 + signatureLen + data.length;

        } else if (command == Command.KADEMLIA_STORE) {

            if (4 + 8 + NodeId.PUBLIC_KEYLEN + 4 + MIN_SIGNATURE_LEN > readBuffer.remaining()) {
                return 0;
            }

            int commandLen = readBuffer.getInt();

            if (commandLen > readBuffer.remaining()) {
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

            if (contentLen <= 0 && contentLen > 1024 * 1024 * 10) {
                peer.disconnect("wrong contentLen for kadcontent");
                return 0;
            }

            byte[] contentBytes = new byte[contentLen];
            //todo use ByteBufferPool
            readBuffer.get(contentBytes);

            if (MIN_SIGNATURE_LEN > readBuffer.remaining()) {
                return 0;
            }

            int signatureLenInBuffer = readBuffer.getInt();

            byte[] signatureBytes = Utils.readSignature(readBuffer);
            if (signatureBytes == null) {
                return 0;
            }
            int lenOfSignature = signatureBytes.length;

            if (signatureLenInBuffer != lenOfSignature) {
                throw new RuntimeException("failure in len of signature: expected " + signatureLenInBuffer + " found in the signature itself: " + lenOfSignature + " lightClient: " + peer.isLightClient());
            }


            KadContent kadContent = new KadContent(timestamp, publicKeyBytes, contentBytes, signatureBytes);

            if (kadContent.verify()) {
                serverContext.getKadStoreManager().put(kadContent);

                peer.getWriteBufferLock().lock();
                try {
                    peer.getWriteBuffer().put(Command.JOB_ACK);
                    peer.getWriteBuffer().putInt(ackId);
                } finally {
                    peer.getWriteBufferLock().unlock();
                }

                /**
                 * Light clients do not look up the dht tables such that we have to insert the KadContent by ourselves.
                 */
                if (peer.isLightClient()) {
                    System.out.println("peer is light client, start KadInserJob!");
                    KademliaInsertJob kademliaInsertJob = new KademliaInsertJob(serverContext, kadContent);
                    kademliaInsertJob.start();
                }

            } else {
                //todo
                System.out.println("kadContent verification failed!!!");
                Log.sentry("kadContent verification failed, lightClient: " + peer.isLightClient());
            }

            return 1 + 4 + commandLen;

        } else if (command == Command.JOB_ACK) {


            int jobId = readBuffer.getInt();

            Job runningJob = Job.getRunningJob(jobId);

            if (runningJob instanceof KademliaInsertJob) {
                KademliaInsertJob job = (KademliaInsertJob) runningJob;
                job.ack(peer);
                System.out.println("ACK from peer: " + peer.getNodeId().toString());
            }


            return 1 + 4;

        } else if (command == Command.KADEMLIA_GET) {

            if (readBuffer.remaining() < 4 + KademliaId.ID_LENGTH_BYTES) {
                return 0;
            }

            int jobId = readBuffer.getInt();

            byte[] kadIdBytes = new byte[KademliaId.ID_LENGTH_BYTES];
            readBuffer.get(kadIdBytes);


            KademliaId searchedId = new KademliaId(kadIdBytes);


            KadContent kadContent = serverContext.getKadStoreManager().get(searchedId);

            if (kadContent != null) {

                peer.getWriteBufferLock().lock();
                try {
                    peer.getWriteBuffer().put(Command.KADEMLIA_GET_ANSWER);
                    peer.getWriteBuffer().putInt(jobId);
                    peer.getWriteBuffer().putLong(kadContent.getTimestamp());
                    peer.getWriteBuffer().put(kadContent.getPubkey());
                    peer.getWriteBuffer().putInt(kadContent.getContent().length);
                    peer.getWriteBuffer().put(kadContent.getContent());
                    peer.getWriteBuffer().put(kadContent.getSignature());
                } finally {
                    peer.getWriteBufferLock().unlock();
                }

            } else {
                new KademliaSearchJobAnswerPeer(serverContext, searchedId, peer, jobId).start();
            }
            return 1 + 4 + KademliaId.ID_LENGTH_BYTES;

        } else if (command == Command.KADEMLIA_GET_ANSWER) {
            return parseKademliaGetAnswer(readBuffer, peer);
        } else if (command == Command.FLASCHENPOST_PUT) {


            int contentLen = readBuffer.getInt();

            if (readBuffer.remaining() < contentLen) {
                return 0;
            }

            byte[] content = new byte[contentLen];
            readBuffer.get(content);

            GMContent gmContent = GMParser.parse(serverContext, content);

            return 1 + 4 + contentLen;

        }


        throw new RuntimeException("Got unknown command from peer: " + command + " last cmd: " + peer.lastCommand + " lightClient: " + peer.isLightClient());

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
                Log.sentry(String.format("command took over 5 seconds to parse: %s", diff));
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
