/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;


import com.google.flatbuffers.FlatBufferBuilder;
import im.redpanda.App;
import im.redpanda.commands.FBPeer;
import im.redpanda.commands.FBPeerList;
import im.redpanda.commands.FBPublicKey;
import im.redpanda.crypt.Base58;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.Utils;
import io.sentry.Sentry;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author robin
 */
public class ConnectionReaderThread extends Thread {

    public static int MAX_THREADS = 30;
    public static int STD_TIMEOUT = 10;
    public static final ArrayList<ConnectionReaderThread> threads = new ArrayList<>();
    public static final ReentrantLock threadLock = new ReentrantLock(false);

    private static ReentrantLock updateUploadLock = new ReentrantLock();

    private boolean run = true;
    private int timeout;
    private ByteBuffer myReaderBuffer = ByteBuffer.allocate(1024 * 50);


    public ConnectionReaderThread(int timeout) {
        this.timeout = timeout;
        Log.putStd("########################## spawned new connectionReaderThread!!!!");
        start();

    }

    public static void init() {
        threadLock.lock();
        threads.add(new ConnectionReaderThread(-1));
        threadLock.unlock();
        Log.putStd("wwoooo");

        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            public void uncaughtException(Thread thread, Throwable thrwbl) {

                Log.putCritical(thrwbl);

            }
        });


    }

    @Override
    public void run() {

        setName("ReaderThread");

        int peekedAndFound = 0;
        int lastThreadSize = 1;
        int threadSize = 1;

        while (!Server.SHUTDOWN && run) {

            threadLock.lock();
            try {
                threadSize = threads.size();
                if (threadSize > MAX_THREADS) {
                    run = false;
                    threads.remove(this);
                    Log.put("threads now: " + threads.size(), -10);
                    continue;
                }

            } finally {
                threadLock.unlock();
            }

            if (threadSize != lastThreadSize) {
                peekedAndFound = 0;
            }

            Peer poll = null;
            try {

                if (timeout == -1) {
                    poll = ConnectionHandler.peersToReadAndParse.take(); // will never return null element, needed for main thread. Dann ist immer einer am Leben.
                } else {
                    poll = ConnectionHandler.peersToReadAndParse.poll(timeout, TimeUnit.SECONDS);
                }

                int size = ConnectionHandler.peersToReadAndParse.size();
                if (size > 20) {
                    System.out.println("too many peers waiting for read: " + size);
                }

//                System.out.println("peekedAndFound: " + peekedAndFound);

//                if (ConnectionHandler.peersToReadAndParse.size() > threads.size()) {
                if (ConnectionHandler.peersToReadAndParse.peek() != null) {

                    if (peekedAndFound < 0) {
                        peekedAndFound = 0;
                    }


                    peekedAndFound++;

                    if (peekedAndFound > 5) {

                        threadLock.lock();
                        if (threads.size() < MAX_THREADS) {

                            try {
                                ConnectionReaderThread connectionReaderThread = new ConnectionReaderThread(STD_TIMEOUT);
                                threads.add(connectionReaderThread);
                                Log.put("threads now: " + threads.size(), -10);
                            } catch (Throwable e) {
                                MAX_THREADS = MAX_THREADS - 1;
                                System.out.println("reducing max threads: " + MAX_THREADS);
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
                System.out.println("ggzdazdndzgrztgr");
                e.printStackTrace();
            }

            lastThreadSize = threadSize;

            if (poll == null) {
                //this thread can be destroyed, a new one will be started if needed
                //Log.putStd("thread timeout!! " + getName());
                run = false;
                threadLock.lock();
                threads.remove(this);
                Log.put("threads now: " + threads.size(), -10);
                threadLock.unlock();
                continue;
            }

//            Log.putStd("a1: " + df.format((double) (System.nanoTime() - time) / 1000000.));
            long a = System.currentTimeMillis();


            readConnection(poll);


            long diff = (System.currentTimeMillis() - a);

            if (diff > 5000L) {
                System.out.println("time: " + diff);
            }

//            Log.putStd("simulate long query");
//            try {
//                sleep(500);
//            } catch (InterruptedException ex) {
//                Logger.getLogger(ConnectionReaderThread.class.getName()).log(Level.SEVERE, null, ex);
//            }
////            Log.put("done read and parsing", 20);
//            try {
//                sleep(500);
//            } catch (InterruptedException ex) {
//                Logger.getLogger(ConnectionReaderThread.class.getName()).log(Level.SEVERE, null, ex);
//            }
            ConnectionHandler.doneRead.add(poll);

//            System.out.println("peer released again for read and write....");

//            System.out.println("wakeup selector from readerthread");
            Server.connectionHandler.selector.wakeup();

        }


    }


    private void readConnection(Peer peer) {


        ByteBuffer writeBuffer = peer.writeBuffer;
        SelectionKey key = peer.selectionKey;


        if (myReaderBuffer.position() != 0) {
            throw new RuntimeException("buffer has to be at position 0, otherwise we would parse data from a different peer.");
        }

        int read = -2;
        try {
            read = peer.getSocketChannel().read(myReaderBuffer);
            Log.put("!!read bytes: " + read, 200);
        } catch (IOException e) {
            e.printStackTrace();
            key.cancel();
            peer.disconnect("could not read...");
            return;
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
            peer.disconnect("dafuq 2332");
            return;
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
        peer.decryptInputdata(myReaderBuffer);


        loopCommands(peer, readBuffer);

//        System.out.println("buffer after parse: " + readBuffer);

        if (peer.readBuffer.position() == 0) {
            ByteBufferPool.returnObject(peer.readBuffer);
            peer.readBuffer = null;
        }

    }

    public static void loopCommands(Peer peer, ByteBuffer readBuffer) {
        readBuffer.flip();

        int parsedBytesLocally = -1;

        while (readBuffer.hasRemaining() && parsedBytesLocally != 0) {

            int newPosition = readBuffer.position(); // lets save the position before touching the buffer

            peer.setLastActionOnConnection(System.currentTimeMillis());
//            Log.put("todo: parse data " + readBuffer.remaining(), 200);
            byte b = readBuffer.get();
            Log.put("command: " + b, 200);


            parsedBytesLocally = parseCommand(b, readBuffer, peer);
            newPosition += parsedBytesLocally;
            readBuffer.position(newPosition);
        }

        readBuffer.compact();
    }

    public static int parseCommand(byte command, ByteBuffer readBuffer, Peer peer) {

        Log.put("cmd: " + command + " " + Command.PING + " " + (command == Command.PING), 200);

        if (command == Command.PING) {
            Log.put("Received ping command", 200);
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

            return 1;
        } else if (command == Command.REQUEST_PEERLIST) {


            PeerList.getReadWriteLock().readLock().lock();
            try {

                int[] peers = new int[PeerList.getPeerArrayList().size()];
                int[] kademliaIds = new int[PeerList.getPeerArrayList().size()];
                int[] ips = new int[PeerList.getPeerArrayList().size()];

                FlatBufferBuilder builder = new FlatBufferBuilder(1024 * 200);

                int cnt = 0;
                for (Peer peerToWrite : PeerList.getPeerArrayList()) {

                    if (peerToWrite.ip == null) {
                        continue;
                    }

//                    FlatBufferBuilder builder2 = new FlatBufferBuilder(1024);

                    if (peerToWrite.getNodeId() != null && peerToWrite.getNodeId().getKademliaId() != null) {
                        kademliaIds[cnt] = builder.createByteVector(peerToWrite.getNodeId().getKademliaId().getBytes());
                    } else {

                    }
                    ips[cnt] = builder.createString(peerToWrite.ip);
                    peers[cnt] = FBPeer.createFBPeer(builder, kademliaIds[cnt], ips[cnt], peerToWrite.getPort());
                    cnt++;
                }


                int peersVector = FBPeerList.createPeersVector(builder, peers);


                int fbPeerList = FBPeerList.createFBPeerList(builder, peersVector);

//                FBPeerList.startFBPeerList(builder);
//                FBPeerList.addPeers(builder, peersVector);
//                int fbPeerList = FBPeerList.endFBPeerList(builder);
                builder.finish(fbPeerList);

                ByteBuffer byteBuffer = builder.dataBuffer();
//                System.out.println("peersoutbuffer: " + byteBuffer);

                peer.getWriteBufferLock().lock();
                try {
                    peer.writeBuffer.put(Command.SEND_PEERLIST);
                    peer.writeBuffer.putInt(byteBuffer.remaining());
                    peer.writeBuffer.put(byteBuffer);
                    peer.setWriteBufferFilled();
                } finally {
                    peer.getWriteBufferLock().unlock();
                }


            } finally {
                PeerList.getReadWriteLock().readLock().unlock();
            }

            return 1;

        } else if (command == Command.SEND_PEERLIST) {

            int toRead = readBuffer.getInt();
            if (readBuffer.remaining() < toRead) {
                return 0;
            }

            byte[] bytesForPeerList = new byte[toRead];
            readBuffer.get(bytesForPeerList);

            FBPeerList rootAsFBPeerList = FBPeerList.getRootAsFBPeerList(ByteBuffer.wrap(bytesForPeerList));

            Log.put("we obtained a peerlist with " + rootAsFBPeerList.peersLength() + " peers....", 20);

            for (int i = 0; i < rootAsFBPeerList.peersLength(); i++) {

                FBPeer fbPeer = rootAsFBPeerList.peers(i);


                ByteBuffer nodeIdBuffer = fbPeer.nodeIdAsByteBuffer();

                Peer newPeer = null;

                if (nodeIdBuffer != null) {
                    byte[] nodeIdBytes = new byte[nodeIdBuffer.remaining()];
                    nodeIdBuffer.get(nodeIdBytes);

                    KademliaId kademliaId = new KademliaId(nodeIdBytes);

                    if (kademliaId.equals(Server.NONCE)) {
                        Log.put("found ourselves in the peerlist", 80);
                        break;
                    }

                    NodeId nodeId = new NodeId(kademliaId);

                    newPeer = new Peer(fbPeer.ip(), fbPeer.port(), nodeId);

                    Node byKademliaId = Node.getByKademliaId(kademliaId);
                    if (byKademliaId != null) {
                        byKademliaId.addConnectionPoint(fbPeer.ip(), fbPeer.port());
                    }

                } else {
                    newPeer = new Peer(fbPeer.ip(), fbPeer.port());
                }

                Peer add = PeerList.add(newPeer);
                if (add == null) {
                    Log.put("new peer added: " + newPeer, 50);
                } else {
                    Log.put("peer was already in peerlist: " + newPeer, 50);


                    System.out.println("added new ConnectionPoint...");
                }


            }

            return 1 + 4 + toRead;


        } else if (command == Command.UPDATE_REQUEST_TIMESTAMP) {
//            System.out.println("UPDATE_REQUEST_TIMESTAMP " + Server.localSettings.getUpdateTimestamp());
            ByteBuffer writeBuffer = peer.getWriteBuffer();
            peer.writeBufferLock.lock();
            try {
                writeBuffer.put(Command.UPDATE_ANSWER_TIMESTAMP);
                writeBuffer.putLong(Server.localSettings.getUpdateTimestamp());
            } finally {
                peer.writeBufferLock.unlock();
            }
            peer.setWriteBufferFilled();
            return 1;
        } else if (command == Command.UPDATE_ANSWER_TIMESTAMP) {


//            System.out.println("UPDATE_ANSWER_TIMESTAMP ");

            if (8 > readBuffer.remaining()) {
                return 0;
            }

            long othersTimestamp = readBuffer.getLong();


//            System.out.println("UPDATE_ANSWER_TIMESTAMP " + othersTimestamp);

//            System.out.println("Update found from: " + new Date(othersTimestamp) + " our version is from: " + new Date(Settings.getMyCurrentVersionTimestamp()));

            if (othersTimestamp < Server.localSettings.getUpdateTimestamp()) {
                System.out.println("WARNING: peer has outdated redPandaj version! " + peer.getNodeId());
            }

            if (othersTimestamp > Server.localSettings.getUpdateTimestamp() && Settings.isLoadUpdates()) {
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        updateUploadLock.lock();
                        try {
                            System.out.println("our version is outdated, we try to download it from this peer!");
                            peer.writeBufferLock.lock();
                            peer.getWriteBuffer().put(Command.UPDATE_REQUEST_CONTENT);
                            peer.writeBufferLock.unlock();
                            peer.setWriteBufferFilled();


                            //lets not download another version in the next x seconds, otherwise our RAM may explode!
                            try {
                                Thread.sleep(10000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } finally {
                            System.out.println("we can now download it from another peer...");
                            updateUploadLock.unlock();
                        }

                    }
                };

                Server.threadPool.submit(runnable);
            }


            return 1 + 8;
        } else if (command == Command.UPDATE_REQUEST_CONTENT) {

            if (Server.localSettings.getUpdateTimestamp() == -1) {
                return 1;
            }

            if (Server.localSettings.getUpdateSignature() == null) {
                System.out.println("we dont have an official signature to upload that update to other peers!");
                return 1;
            }


            Runnable runnable = new Runnable() {
                @Override
                public void run() {

                    updateUploadLock.lock();

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

                            System.out.println("timestamp: " + Server.localSettings.getUpdateTimestamp());

                            ByteBuffer a = ByteBuffer.allocate(1 + 8 + 4 + Server.localSettings.getUpdateSignature().length + data.length);
                            a.put(Command.UPDATE_ANSWER_CONTENT);
                            a.putLong(Server.localSettings.getUpdateTimestamp());
                            a.putInt(data.length);
                            a.put(Server.localSettings.getUpdateSignature());
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
                            while (cnt < 6) {
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
                        updateUploadLock.unlock();
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

            //second byte of encoding gives the remaining bytes of the signature, cf. eg. https://crypto.stackexchange.com/questions/1795/how-can-i-convert-a-der-ecdsa-signature-to-asn-1
            readBuffer.get();
            int lenOfSignature = ((int) readBuffer.get()) + 2;
            readBuffer.position(readBuffer.position() - 2);

            byte[] signature = new byte[lenOfSignature];
            readBuffer.get(signature);

//            System.out.println("download in pipe: " + new Date(othersTimestamp));

            if (toReadBytes > readBuffer.remaining()) {
                if (Math.random() < 0.05) {
                    System.out.println("Update progress: " + (int) ((double) readBuffer.remaining() / (double) toReadBytes * 100.) + " %");
                }
                return 0;
            }

            System.out.println("update completely in buffer!");

            byte[] data = new byte[toReadBytes];
            readBuffer.get(data);


            if (othersTimestamp > Server.localSettings.getUpdateTimestamp() && !Settings.isSeedNode()) {
                System.out.println("we got the update successfully, install it! timestamp: " + othersTimestamp);


                System.out.println("signature found: " + Utils.bytesToHexString(signature) + " len: " + signature.length);
                System.out.println("hash data: " + Sha256Hash.create(data));

                //lets check the signature chunk:
                NodeId nodeId = Updater.getPublicUpdaterKey();


                ByteBuffer bytesToHash = ByteBuffer.allocate(8 + toReadBytes);

                bytesToHash.putLong(othersTimestamp);
                bytesToHash.put(data);

                boolean verified = nodeId.verify(bytesToHash.array(), signature);


                System.out.println("update verified: " + verified);

                File file = new File("redpanda.jar");
                long myCurrentVersionTimestamp = file.lastModified();
                if (!file.exists()) {
                    System.out.println("No jar to update found, exiting auto update!");
                    return 1 + 8 + 4 + lenOfSignature + data.length;
                }

                if (myCurrentVersionTimestamp >= othersTimestamp) {
                    System.out.println("update not required our file is newer or equal, aborting...");
                    return 1 + 8 + 4 + lenOfSignature + data.length;
                }

                if (Server.localSettings.getUpdateTimestamp() >= othersTimestamp) {
                    System.out.println("update not required our update timestamp is newer or equal, aborting...");
                    return 1 + 8 + 4 + lenOfSignature + data.length;
                }


                if (verified) {

                    try (FileOutputStream fos = new FileOutputStream("update")) {
                        fos.write(data);
                        //fos.close(); There is no more need for this line since you had created the instance of "fos" inside the try. And this will automatically close the OutputStream
                        System.out.println("update store in update file");

                        File f = new File("update");
                        f.setLastModified(othersTimestamp);

                        Server.localSettings.setUpdateSignature(signature);
                        Server.localSettings.setUpdateTimestamp(othersTimestamp);
                        Server.localSettings.save(Server.MY_PORT);

                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.exit(0);
                    } catch (FileNotFoundException e) {
                        Sentry.capture(e);
                        e.printStackTrace();
                    } catch (IOException e) {
                        Sentry.capture(e);
                        e.printStackTrace();
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

        }

        return 0;
    }


    public static void sendHandshake(PeerInHandshake peerInHandshake) {

        ByteBuffer writeBuffer = ByteBufferPool.borrowObject(30);

        writeBuffer.put(Server.MAGIC.getBytes());
        writeBuffer.put((byte) Server.VERSION);
        writeBuffer.put(Server.NONCE.getBytes());
        writeBuffer.putInt(Server.MY_PORT);

        writeBuffer.flip();

        try {
            int write = peerInHandshake.getSocketChannel().write(writeBuffer);
//            System.out.println("written bytes of handshake: " + write);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                peerInHandshake.getSocketChannel().close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        ByteBufferPool.returnObject(writeBuffer);
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

    public static void sendPublicKeyToPeer(PeerInHandshake peerInHandshake) {


        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        int publicKeyBytes = builder.createByteVector(Server.nodeId.exportPublic());
        int sendPublicKey = FBPublicKey.createFBPublicKey(builder, publicKeyBytes);
        builder.finish(sendPublicKey);
        ByteBuffer byteBuffer = builder.dataBuffer();

        ByteBuffer commandBuffer = ByteBuffer.allocate(1);

        commandBuffer.put(Command.SEND_PUBLIC_KEY);
        commandBuffer.flip();

        ByteBuffer[] buffers = new ByteBuffer[2];
        buffers[0] = commandBuffer;
        buffers[1] = byteBuffer;

        try {
            long write = peerInHandshake.getSocketChannel().write(buffers);
            System.out.println("written bytes to SEND_PUBLIC_KEY: " + write);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean parseHandshake(PeerInHandshake peerInHandshake, ByteBuffer buffer) {


        if (buffer.remaining() < 29) {
            System.out.println("not enough bytes for handshake");
            return false;
        }


        String magic = readString(buffer, 4);
//        System.out.println("magic: " + magic);

        int version = (int) buffer.get();

//        System.out.println("version: " + version);

        byte[] nonceBytes = new byte[KademliaId.ID_LENGTH / 8];
        buffer.get(nonceBytes);

        KademliaId identity = new KademliaId(nonceBytes);

//        System.out.println("identity: " + identity.toString());

        int port = buffer.getInt();

//        System.out.println("port: " + port);

        peerInHandshake.setIdentity(identity);

        peerInHandshake.setPort(port);

        if (port < 0 || port > 65535) {
            System.out.println("wrong port...");
            return false;
        }

        Log.put("Verbindungsaufbau (" + peerInHandshake.ip + "): " + magic + " " + version + " " + identity.toString() + " " + port + " initByMe: ", 10);

        buffer.compact();

        if (identity.equals(Server.NONCE)) {
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
                boolean b = PeerList.removeIpPort(peerInHandshake.ip, peerInHandshake.port);
                System.out.println("remove of peer successful?: " + b);
            }
            return false;
        }

        /**
         * If the connection was not initialized by us we have to find the peer first for this handshake.
         */
        if (peerInHandshake.getPeer() == null) {
            Peer peer = PeerList.get(identity);
            if (peer == null) {
                //No peer found with this identity, lets create a new Peer instance and add it to the list
                peer = new Peer(peerInHandshake.ip, peerInHandshake.port);
                peer.setNodeId(new NodeId(identity));
//                peer.setKademliaId(identity);
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
                    PeerList.updateKademliaId(peerInHandshake.getPeer(), identity);
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
                    peerInHandshake.getPeer().clearConnectionDetails();
                    PeerList.add(new Peer(peerInHandshake.ip, peerInHandshake.port));
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

            Peer peer = PeerList.get(identity);
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


    public static String readString(ByteBuffer byteBuffer, int length) {

        if (byteBuffer.limit() - byteBuffer.arrayOffset() < length) {
            return null; //not enough bytes rdy!
        }
        byteBuffer.position(byteBuffer.position() + length);
        return new String(byteBuffer.array(), byteBuffer.arrayOffset(), length);
    }

}
