/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.security.SecureRandom;
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

    public static final int MAX_ELO_VALUE = 12000 * 10;
    public static int MAX_THREADS = 30;
    public static int STD_TIMEOUT = 10;
    public static int ROUNDS_TO_PLAY = 5;
    public static final ArrayList<ConnectionReaderThread> threads = new ArrayList<>();
    public static final ReentrantLock threadLock = new ReentrantLock(false);
    public SecureRandom secureRandom = new SecureRandom();
    public Random random = new Random();
    public static AtomicInteger searchCounter = new AtomicInteger(100);

    public static final HashMap<Byte, Long> parseStats = new HashMap<>();

    private boolean run = true;
    private int timeout;


    public static final boolean noBotsAllowed = false;

    static {

        new Thread() {
            @Override
            public void run() {

                while (!Server.SHUTDOWN) {

                    try {
                        sleep(10000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(ConnectionReaderThread.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    int addAndGet = searchCounter.addAndGet(-50);
                    if (addAndGet < 0) {
                        searchCounter.set(0);
                    }
                }

            }

        }.start();

    }

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


        while (!Server.SHUTDOWN && run) {

            threadLock.lock();
            try {

                if (threads.size() > MAX_THREADS) {
                    run = false;
                    threads.remove(this);
                    Log.put("threads now: " + threads.size(), -10);
                    continue;
                }

            } finally {
                threadLock.unlock();
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

//                if (ConnectionHandler.peersToReadAndParse.size() > threads.size()) {
                if (ConnectionHandler.peersToReadAndParse.peek() != null) {
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
                }

            } catch (InterruptedException ex) {
                Log.putStd("interrupted, finish thread...");
                run = false;
                continue;
            }

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
            Server.connectionHandler.selector.wakeup();

        }


    }


    private void readConnection(Peer peer) {

        ByteBuffer readBuffer = peer.readBuffer;
        ByteBuffer writeBuffer = peer.writeBuffer;
        SelectionKey key = peer.selectionKey;


        int read = -2;
        try {
            read = peer.getSocketChannel().read(peer.readBufferCrypted);
            System.out.println("!!read bytes: " + read);
        } catch (IOException e) {
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
            Log.put("closing connection " + peer.ip + ": not readable! " + readBuffer, 200);
            peer.disconnect(" read == -1 ");
            key.cancel();
        } else {

            System.out.println("received bytes!");


        }


        //no encryption
        peer.readBufferCrypted.flip();
        peer.readBuffer.put(peer.readBufferCrypted);
        peer.readBufferCrypted.compact();


        readBuffer.flip();

        System.out.println("todo: parse data " + readBuffer.remaining());

        readBuffer.compact();

    }


    public static void sendHandshake(PeerInHandshake peerInHandshake) {

        ByteBuffer writeBuffer = ByteBuffer.allocate(30);

        writeBuffer.put(Server.MAGIC.getBytes());
        writeBuffer.put((byte) Server.VERSION);
        writeBuffer.put(Server.NONCE.getBytes());
        writeBuffer.putInt(Server.MY_PORT);

        writeBuffer.flip();

        try {
            int write = peerInHandshake.getSocketChannel().write(writeBuffer);
            System.out.println("written bytes of handshake: " + write);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean parseHandshake(PeerInHandshake peer, ByteBuffer buffer) {


        buffer.flip();

        System.out.println(buffer);

        String magic = readString(buffer, 4);
        int version = (int) buffer.get();

        byte[] nonceBytes = new byte[KademliaId.ID_LENGTH / 8];
        buffer.get(nonceBytes);

        KademliaId identity = new KademliaId(nonceBytes);

        int port = buffer.getInt();

        peer.setIdentity(identity);
        peer.setPort(port);

        Log.put("Verbindungsaufbau (" + peer.ip + "): " + magic + " " + version + " " + identity + " " + port + " initByMe: ", 10);

        buffer.compact();

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
