/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;


import im.redpanda.commands.FBPublicKey;
import io.sentry.Sentry;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.security.SecureRandom;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author robin
 */
public class ConnectionHandler extends Thread {

    public static final boolean ENCRYPTION_ENABLED = false;

    public static Selector selector;
    public static final ReentrantLock selectorLock = new ReentrantLock();

    public static ArrayList<Peer> peerList = new ArrayList<>();
    public static ReentrantLock allSocketsLock = new ReentrantLock(false);
    public static BlockingQueue<Peer> peersToReadAndParse = new LinkedBlockingQueue<>(600);
    public static ArrayList<Peer> workingRead = new ArrayList<>();
    public static BlockingQueue<Peer> doneRead = new LinkedBlockingQueue<>(600);
    public static long time;
    public static DecimalFormat df = new DecimalFormat("#.000");


    static {

        try {
            selector = Selector.open();
        } catch (IOException ex) {
            ex.printStackTrace();
        }


    }


    public void bind() {


        try {
            ServerSocketChannel serverSocketChannel;
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);

            boolean bound = false;
            //MY_PORT = Settings.STD_PORT;
            Server.MY_PORT = Settings.getStartPort();
            ServerSocket serverSocket = null;


            System.out.println("searching port to bind to...");


            while (!bound) {

                bound = true;
                try {
                    serverSocketChannel.socket().bind(new InetSocketAddress(Server.MY_PORT));
                } catch (Throwable e) {


                    System.out.println("could not bound to port: " + Server.MY_PORT);


                    //e.printStackTrace();
                    bound = false;
                    //MY_PORT = Settings.STD_PORT + random.nextInt(30);
                    Server.MY_PORT += 1;
                }

            }

            System.out.println("bound successfuly to port: " + Server.MY_PORT);


            addServerSocketChannel(serverSocketChannel);
//                startedUpSuccessful();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    void addServerSocketChannel(ServerSocketChannel serverSocketChannel) {
        try {
            selector.wakeup();
            SelectionKey key = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            selector.wakeup();
            Log.putStd("added ServerSocketChannel");

        } catch (IOException ex) {
            Logger.getLogger(ConnectionHandler.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void run() {

        final String orgName = Thread.currentThread().getName();
        if (!orgName.contains(" ")) {
            Thread.currentThread().setName(orgName + " - IncomingHandler - Main");
        }

        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            public void uncaughtException(Thread thread, Throwable thrwbl) {

                Log.putCritical(thrwbl);

            }
        });

        bind();

        ConnectionReaderThread.init();

        while (!Server.SHUTDOWN) {

            Peer p;

            time = System.nanoTime();

            while ((p = doneRead.poll()) != null) {
                try {

                    //Log.putStd("current interests: " + p.getSelectionKey().interestOps());
//ToDo: optimize
                    //if (p.writeBuffer != null && p.writeBuffer.hasRemaining()) {
                    p.writeBufferLock.lock();
                    try {
                        p.setWriteBufferFilled();
                    } finally {
                        p.writeBufferLock.unlock();
                    }
                    //}

                    p.getSelectionKey().interestOps(p.getSelectionKey().interestOps() | SelectionKey.OP_READ);
                    workingRead.remove(p);
                } catch (CancelledKeyException e) {
                    Log.putStd("key was canneled");
                }
            }

            //Log.putStd("1: " + df.format((double)(System.nanoTime() - time)/ 1000000.));
            Log.put("NEW KEY RUN - before select", 2000);
            int readyChannels = 0;
            try {
                selectorLock.lock();
                selectorLock.unlock();
                readyChannels = selector.select();
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    sleep(100);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                continue;
            }

            time = System.nanoTime();
            //Log.putStd("2: " + df.format((double)(System.nanoTime() - time)/ 1000000.));

            //Log.put("NEW KEY RUN - after select", 2000);
            Set<SelectionKey> selectedKeys = selector.selectedKeys();

            if (readyChannels == 0 && selectedKeys.isEmpty()) {
                //Log.putStd("asd");
//                try {
//                    sleep(100);
//                } catch (InterruptedException ex) {
//                    Logger.getLogger(ConnectionHandler.class.getName()).log(Level.SEVERE, null, ex);
//                }
                continue;
            }

            //Log.putStd("3: " + df.format((double)(System.nanoTime() - time)/ 1000000.));
            Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

            while (keyIterator.hasNext()) {

                SelectionKey key = keyIterator.next();
                keyIterator.remove();
                if (!key.isValid()) {
                    Log.putStd("key was invalid");
                    key.cancel();
                    //keyIterator.remove();
                    continue;
                }

                try {

                    if (key.isAcceptable()) {
                        // a connection was accepted by a ServerSocketChannel.

//                        Log.put("incoming connection...", 12);

                        ServerSocketChannel s = (ServerSocketChannel) key.channel();
                        SocketChannel socketChannel = s.accept();
                        socketChannel.configureBlocking(false);
                        String ip = socketChannel.socket().getInetAddress().getHostAddress();

                        String[] split = ip.split("\\.");
                        if (split.length == 4) {
                            ip = split[0] + "." + split[1] + "." + split[2] + "." + split[3];
                        } else {
                            ip = "not4blocks";
                        }

                        Log.put("incoming connection from ip: " + ip, 12);

                        try {
                            socketChannel.configureBlocking(false);

                            selector.wakeup();
                            SelectionKey newKey = socketChannel.register(selector, SelectionKey.OP_READ);

                            PeerInHandshake peerInHandshake = new PeerInHandshake(ip, socketChannel);

                            ConnectionReaderThread.sendHandshake(peerInHandshake);

                            newKey.attach(peerInHandshake);
                            peerInHandshake.setKey(newKey);
                            selector.wakeup();
                            //Log.putStd("added con");
                        } catch (IOException ex) {
                            ex.printStackTrace();
                            Log.putStd("could not init connection....");
                            return;
                        }

//                        Peer peer1 = new Peer(ip);
//                        peer1.setSocketChannel(socketChannel);
//                        addConnection(peer1, false);
                        continue;
                    }


                    if (key.attachment() instanceof PeerInHandshake) {
                        PeerInHandshake peerInHandshake = (PeerInHandshake) key.attachment();

                        if (key.isConnectable()) {
                            boolean connected = false;
                            try {
                                connected = peerInHandshake.getSocketChannel().finishConnect();
                            } catch (IOException | SecurityException e) {
                                e.printStackTrace();
                            }
//                            Log.putStd("finished!");

                            if (!connected) {
                                Log.put("connection could not be established...", 150);
                                key.cancel();
//                                peer.disconnect("connection could not be established");
                                continue;
                            }

                            Log.putStd("Connection established...");
                            ConnectionReaderThread.sendHandshake(peerInHandshake);
                        }
                        if (key.isReadable()) {

                            /**
                             * Lets read that data from the other Peer.
                             */
                            ByteBuffer allocate = ByteBuffer.allocate(117);
                            int read = peerInHandshake.getSocketChannel().read(allocate);
                            if (read == -1) {
                                System.out.println("peer disconnected...");
                                key.cancel();
                                continue;
                            } else if (read == 0) {
                                System.out.println("read zero bytes...");
                                continue;
                            }

                            allocate.flip();

                            if (!peerInHandshake.isEncryptionActive()) {

                                Log.put("read: " + read + " " + key.interestOps(), 150);
                                if (peerInHandshake.getStatus() == 0) {
                                    /**
                                     * The status indicates that no handshake was parsed before for this PeerInHandshake
                                     */
                                    boolean b = ConnectionReaderThread.parseHandshake(peerInHandshake, allocate);
                                    System.out.println("handshake okay?: " + b);
                                } else {

                                    /**
                                     * The status indicates that the first handshake was already parsed before for this
                                     * PeerInHandshake. Here we are providing more data for the other Peer like the public key.
                                     */
                                    byte command = allocate.get();
                                    if (command == Command.REQUEST_PUBLIC_KEY) {
                                        /**
                                         * The other Peer request our public key, lets send our public key!
                                         */
                                        ConnectionReaderThread.sendPublicKeyToPeer(peerInHandshake);
                                    } else if (command == Command.SEND_PUBLIC_KEY && peerInHandshake.getStatus() == 1) {
                                        /**
                                         * We got the public Peer, lets store it and check that this public key
                                         * indeed corresponds to the KademliaId.
                                         */
                                        FBPublicKey rootAsSendPublicKey = FBPublicKey.getRootAsFBPublicKey(allocate);
                                        ByteBuffer byteBuffer = rootAsSendPublicKey.publicKeyAsByteBuffer();

                                        byte[] bytes = new byte[NodeId.PUBLIC_KEYLEN];
                                        byteBuffer.get(bytes);
                                        NodeId nodeId = NodeId.importPublic(bytes);
                                        System.out.println("new nodeid from peer: " + nodeId.getKademliaId());

                                        if (!peerInHandshake.getIdentity().equals(nodeId.getKademliaId())) {
                                            /**
                                             * We obtained a public key which does not match the KademliaId of this Peer
                                             * and should cancel that connection here.
                                             */
                                            peerInHandshake.setStatus(2);
                                            peerInHandshake.getSocketChannel().close();
                                        } else {
                                            /**
                                             * We obtained the correct public key and can add it to the Peer
                                             * and lets set that peerInHandshake status to waiting for encryption
                                             */
                                            peerInHandshake.getPeer().setNodeId(nodeId);
                                            peerInHandshake.setNodeId(nodeId);
                                            peerInHandshake.setStatus(-1);
                                        }

                                    } else if (command == Command.ACTIVATE_ENCRYPTION) {


                                        /**
                                         * We received the byte to activate the encryption and we are awaiting the encryption activation byte
                                         */

                                        //lets read the random bytes from them

                                        if (allocate.remaining() < PeerInHandshake.IVbytelen / 2) {
                                            System.out.println("not enough bytes for encryption...");
                                            peerInHandshake.getSocketChannel().close();
                                        }

                                        byte[] randomBytesFromThem = new byte[PeerInHandshake.IVbytelen / 2];
                                        allocate.get(randomBytesFromThem);

                                        peerInHandshake.setRandomFromThem(randomBytesFromThem);

                                        peerInHandshake.setAwaitingEncryption(true);

                                    }

                                    /**
                                     * Lets check if we are ready to start the encryption for this handshaking peer
                                     */
                                    if (peerInHandshake.getStatus() == -1 && !peerInHandshake.isWeSendOurRandom()) {
                                        ByteBuffer activateEncryptionBuffer = ByteBuffer.allocate(1 + PeerInHandshake.IVbytelen / 2);
                                        activateEncryptionBuffer.put(Command.ACTIVATE_ENCRYPTION);

                                        activateEncryptionBuffer.put(peerInHandshake.getRandomFromUs());

                                        activateEncryptionBuffer.flip();

                                        long write = peerInHandshake.getSocketChannel().write(activateEncryptionBuffer);
                                        System.out.println("written bytes for ACTIVATE_ENCRYPTION: " + write);
                                        peerInHandshake.setWeSendOurRandom(true);
                                    }

                                    if (peerInHandshake.getStatus() == -1 && peerInHandshake.isAwaitingEncryption() && peerInHandshake.hasPublicKey()) {
                                        peerInHandshake.setAwaitingEncryption(false);

                                        System.out.println("lets generate the shared secret");

                                        peerInHandshake.calculateSharedSecret();

                                        /**
                                         * Shared Secret and IV calculated via ECDH and random bytes,
                                         * lets activate the encryption
                                         */

                                        peerInHandshake.activateEncryption();


                                        /**
                                         * lets send the first ping
                                         */

                                        byte[] bytesSendToPing = new byte[1];
                                        bytesSendToPing[0] = Command.PING;


                                        byte[] encrypt = peerInHandshake.encrypt(bytesSendToPing);
                                        ByteBuffer wrap = ByteBuffer.wrap(encrypt);

                                        try {
                                            int write = peerInHandshake.getSocketChannel().write(wrap);
                                            System.out.println("written bytes for PING: " + write);
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }


                                    }

                                }
                            } else {
                                /**
                                 * The encryption is active in this section, lets check that first ping
                                 */
                                System.out.println("received first encrypted command...");

//                                allocate.flip();
                                byte[] bytesToDecrypt = new byte[allocate.remaining()];
                                allocate.get(bytesToDecrypt);

                                ByteBuffer wrap = ByteBuffer.wrap(peerInHandshake.decrypt(bytesToDecrypt));

                                byte decryptedCommand = wrap.get();

                                if (decryptedCommand == Command.PING) {
                                    System.out.println("received first ping...");

                                    /**
                                     * We can now safely transfer the open connection from the peerInHandshake to the
                                     * actual peer
                                     */

                                    Peer peer = peerInHandshake.getPeer();

                                    peer.setupConnection(peerInHandshake);
                                    continue;


                                }


                            }

                        }

                        continue;
                    }


                    Peer peer = (Peer) key.attachment();
                    if (peer == null) {
                        key.cancel();
                        continue;
                    }
                    ByteBuffer readBuffer = peer.readBuffer;
                    //ByteBuffer writeBuffer = peer.writeBufferCrypted;

                    if (!key.isValid()) {
                        peer.disconnect("key is invalid.");
                        continue;
                    }

                    if (key.isConnectable()) {

                        //Log.putStd("dwdhjawdgawgd ");
                        boolean connected = false;
                        try {
                            connected = peer.getSocketChannel().finishConnect();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (SecurityException e) {
                            e.printStackTrace();
                        }
//                            Log.putStd("finished!");

                        if (!connected) {
                            Log.put("connection could not be established...", 150);
                            key.cancel();
                            peer.disconnect("connection could not be established");
                            continue;
                        }

                        Log.putStd("Connection established...");
//                        sendHandshake(peer);
                        key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);


                        // a connection was established with a remote server.
                    } else if (key.isReadable()) {

                        int interestOps = key.interestOps();

                        //Log.putStd("interestsssssssssssss: " + interestOps);
                        if ((interestOps == (SelectionKey.OP_WRITE | SelectionKey.OP_READ))) {
                            key.interestOps(SelectionKey.OP_WRITE);
                        } else if (interestOps == (SelectionKey.OP_READ)) {
                            key.interestOps(0);
                        } else {
                            System.out.println("adszaudgwzqanzauzgzwzeuzgrgewgsbfsvdhfs " + interestOps);
                            key.interestOps(0);
//                            key.cancel();
                        }

                        if (!workingRead.contains(peer)) {
                            workingRead.add(peer);
                            peersToReadAndParse.add(peer);
                            //Log.putStd("4: " + df.format((double) (System.nanoTime() - time) / 1000000.));
                        } else {
                            //Log.putStd("asde2fsdfcv546tv54bv6");
                            //throw new RuntimeException("asde2fsdfcv546tv54bv6");
                        }

                    } else if (key.isWritable()) {

//                        Log.putStd("key is writeAble");
                        peer.writeBufferLock.lock();

                        try {

                            int writtenBytes = 0;
                            boolean remainingBytes = false;


                            /**
                             * First encrypt all bytes from the writebuffer to the writebuffercrypted...
                             * todo: this should be done in a seperate thread/threadpool...
                             */
                            peer.encrypteOutputdata();


                            peer.writeBufferCrypted.flip();
                            remainingBytes = peer.writeBufferCrypted.hasRemaining();
                            peer.writeBufferCrypted.compact();

//                            Log.putStd("remainingBytes: " + remainingBytes);

                            //switch buffer for reading
                            if (!remainingBytes) {
                                key.interestOps(SelectionKey.OP_READ);
                                //Log.putStd("removed OP_WRITE");
                            } else {
                                //Log.putStd("write from buffer...");

                                try {
                                    writtenBytes = peer.writeBytesToPeer(peer.writeBufferCrypted);
                                } catch (IOException e) {
                                    Log.putStd("could not write bytes to peer, peer disconnected?");
                                    peer.disconnect("could not write");
                                    continue; //finally unlocks the lock
                                }
//                            Log.put("written: " + writtenBytes, 40);
                            }

//                                peer.writeBuffer.flip(); //switch buffer for writing
//                                writeBuffer.limit(writeBuffer.capacity());
//                            writeBuffer.limit(limit);
//                            writeBuffer.position(position - writtenBytes);

                            //Log.putStd("wrote from buffer... " + writtenBytes + " ip: " + peer.ip);
                            Server.outBytes += writtenBytes;
                            peer.sendBytes += writtenBytes;
                        } finally {
                            peer.writeBufferLock.unlock();
                        }

                    }

                } catch (IOException e) {
                    key.cancel();
                    if (key.attachment() instanceof PeerInHandshake) {
                        PeerInHandshake peerInHandshake = ((PeerInHandshake) key.attachment());
                        Log.putStd("error! " + peerInHandshake.ip);
//                        peer.disconnect("IOException");
                    } else if (key.attachment() instanceof Peer) {
                        Peer peer = ((Peer) key.attachment());
                        Log.putStd("error! " + peer.ip);
                        peer.disconnect("IOException");
                    }

                    e.printStackTrace();


                } catch (Throwable e) {
                    key.cancel();
//                    Peer peer = ((Peer) key.attachment());
//                    Log.putStd("Catched fatal exception! " + peer.ip);
                    e.printStackTrace();
                    Sentry.capture(e);
                    //peer.disconnect(" IOException 4827f3fj");
//                    peer.disconnect("Fatal exception");
//                    Log.putCritical(e);
                }

            }
        }

        Log.putStd(
                "ConnectionHandler thread died...");

    }


    public void addConnection(Peer peer, boolean connectionPending) {
        peer.lastActionOnConnection = System.currentTimeMillis();

        allSocketsLock.lock();
        peerList.add(peer);
        allSocketsLock.unlock();

        peer.writeBufferLock.lock();
        try {
            peer.readBuffer = ByteBuffer.allocate(10 * 1024);
            peer.readBufferCrypted = ByteBuffer.allocate(10 * 1024);
            peer.writeBuffer = ByteBuffer.allocate(10 * 1024);
            peer.writeBufferCrypted = ByteBuffer.allocate(10 * 1024);
        } catch (Throwable e) {
            Log.putStd("Speicher konnte nicht reserviert werden. Disconnect peer...");
            peer.disconnect("Speicher konnte nicht reserviert werden.");
        } finally {
            peer.writeBufferLock.unlock();
        }

        try {
            SocketChannel socketChannel = peer.getSocketChannel();
            socketChannel.configureBlocking(false);

            SelectionKey key = null;
            selectorLock.lock();
            try {
                selector.wakeup();
                if (connectionPending) {
                    peer.isConnecting = true;
                    peer.setConnected(false);
                    key = socketChannel.register(selector, SelectionKey.OP_CONNECT);
                } else {
                    peer.isConnecting = false;
                    peer.setConnected(true);
                    key = socketChannel.register(selector, SelectionKey.OP_READ);
                }
            } finally {
                selectorLock.unlock();
            }


            key.attach(peer);
            peer.setSelectionKey(key);
            selector.wakeup();
            System.out.println("c");
            //Log.putStd("added con");
        } catch (IOException ex) {
            ex.printStackTrace();
            peer.disconnect("could not init connection....");
            return;
        }
        Log.putStd("done add");

    }


}
