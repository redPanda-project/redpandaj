/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;




import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
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

    public Selector selector;
    final ReentrantLock selectorLock = new ReentrantLock();

    public static ArrayList<Peer> peerList = new ArrayList<>();
    public static ReentrantLock allSocketsLock = new ReentrantLock(false);
    public static BlockingQueue<Peer> peersToReadAndParse = new LinkedBlockingQueue<>(600);
    public static ArrayList<Peer> workingRead = new ArrayList<>();
    public static BlockingQueue<Peer> doneRead = new LinkedBlockingQueue<>(600);
    public static long time;
    public static DecimalFormat df = new DecimalFormat("#.000");
    public Random random = new Random();


    public ConnectionHandler() {

        try {
            selector = Selector.open();
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
                            ip = split[0] + "." + split[1] + "." + split[2] + "." + random.nextInt(255);
                        } else {
                            ip = "not4blocks";
                        }

                        Log.put("incoming connection from ip: " + ip, 12);

                        try {
                            socketChannel.configureBlocking(false);

                            selector.wakeup();
                            SelectionKey newKey = socketChannel.register(selector, SelectionKey.OP_READ);

                            PeerInHandshake peerInHandshake = new PeerInHandshake(ip);

                            newKey.attach(peerInHandshake);
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
                        } catch (SecurityException e) {
                        }
//                            Log.putStd("finished!");

                        if (!connected) {
                            Log.put("connection could not be established...", 150);
                            key.cancel();
                            peer.disconnect("connection could not be established");
                            continue;
                        }

                        Log.putStd("Connection established...");
                        sendHandshake(peer);
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
                            throw new RuntimeException("adszaudgwzqanzauzgzwzeuzgrgewgsbfsvdhfs");
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

                        Log.putStd("key is writeAble");
                        peer.writeBufferLock.lock();

                        try {

                            if (peer.writeBufferCrypted == null) {
                                try {
                                    if (key.isValid()) {
                                        if (key.interestOps() == SelectionKey.OP_READ) {
                                            key.interestOps(SelectionKey.OP_READ);
                                        } else {
                                            key.interestOps(0);
                                        }
                                    }
                                } catch (CancelledKeyException e) {
                                }
                                peer.writeBufferLock.unlock();
                                Log.put("writebuffer was null and writeable?", 0);
                                continue;
                            }

//                                int position = writeBuffer.position();
//                                int limit = writeBuffer.limit();
                            int writtenBytes = 0;
                            boolean remainingBytes = false;


//                        if (peer.writeBufferCrypted == null) {
//                            remainingBytes = peer.writeBuffer.hasRemaining();
//                        } else {
//                            peer.writeBufferCrypted.flip();
//                            remainingBytes = (peer.writeBuffer.hasRemaining() || peer.writeBufferCrypted.hasRemaining());
//                            peer.writeBufferCrypted.compact();
//                        }
                            peer.writeBufferCrypted.flip();
                            remainingBytes = peer.writeBufferCrypted.hasRemaining();
                            peer.writeBufferCrypted.compact();

                            Log.putStd("remainingBytes: " + remainingBytes);
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
                    Peer peer = ((Peer) key.attachment());
                    Log.putStd("error! " + peer.ip);
                    e.printStackTrace();

                    peer.disconnect("IOException");

                } catch (Throwable e) {
                    key.cancel();
                    Peer peer = ((Peer) key.attachment());
                    Log.putStd("Catched fatal exception! " + peer.ip);
                    e.printStackTrace();
                    //peer.disconnect(" IOException 4827f3fj");
                    peer.disconnect("Fatal exception");
                    Log.putCritical(e);
                }

            }
        }

        Log.putStd(
                "ConnectionHandler thread died...");

    }

    private void sendHandshake(Peer peer) {
        ByteBuffer writeBuffer = peer.writeBufferCrypted;
        peer.writeBufferLock.lock();
        writeBuffer.put(Server.MAGIC.getBytes());
        writeBuffer.put((byte) Server.VERSION);
        writeBuffer.put(Server.NONCE.getBytes());
        writeBuffer.putInt(Server.MY_PORT);
        peer.writeBufferLock.unlock();
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


    static class PeerInHandshake {

        String ip;
        int status = 0;

        public PeerInHandshake(String ip) {
            this.ip = ip;
        }
    }

}
