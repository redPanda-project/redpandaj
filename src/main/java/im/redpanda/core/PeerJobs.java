package im.redpanda.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;

public class PeerJobs extends Thread {

    private final ServerContext serverContext;
    private final PeerList peerList;

    public PeerJobs(ServerContext serverContext) {
        this.serverContext = serverContext;
        this.peerList = serverContext.getPeerList();
    }

    @Override
    public void run() {

        final String orgName = Thread.currentThread().getName();
        Thread.currentThread().setName(orgName + " - ChronJobs for peer communication");

        long lastSaved = System.currentTimeMillis();

        try {
            sleep(3000);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }

        while (!Server.shuttingDown) {

            try {
                sleep(1000 + Server.secureRandom.nextInt(4000));
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }

            if (peerList.size() == 0) {
                continue;
            }

            ConnectionHandler.peerInHandshakesLock.lock();
            try {
                long currentTimeMillis = System.currentTimeMillis();
                ArrayList<PeerInHandshake> toRemove = new ArrayList<>();
                for (PeerInHandshake peerInHandshake : ConnectionHandler.peerInHandshakes) {
                    if (currentTimeMillis - peerInHandshake.getCreatedAt() > 1000L * 10L) {
//                        if (peerInHandshake.getIdentity() != null) {
//                            System.out.println("closing peer in handshake: " + peerInHandshake.getIdentity().toString());
//                        } else {
//                            System.out.println("closing peer in handshake: " + peerInHandshake.getIp());
//                        }
                        try {
                            peerInHandshake.getSocketChannel().close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        toRemove.add(peerInHandshake);
                    }
                }
                ConnectionHandler.peerInHandshakes.removeAll(toRemove);
            } finally {
                ConnectionHandler.peerInHandshakesLock.unlock();
            }

            Lock lock = peerList.getReadWriteLock().readLock();
            lock.lock();
            try {
                for (Peer p : peerList.getPeerArrayList()) {

                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    Log.put("running over peer: " + p, 120);

//                    if (p.getLastAnswered() > 1000 * 60 * 60 * 24 * 7 && PeerList.size() > 3 && p.lastActionOnConnection != 0) {
//                        PeerList.remove(p);
//                    }

//                    if (p.lastPinged - p.lastActionOnConnection > Settings.pingDelay * 2 + 30000
//                            || (p.isConnecting && p.getLastAnswered() > 10000)
//                            || (!p.isConnected() && p.getLastAnswered() > Settings.pingDelay)) {

                    if ((p.isConnecting && p.getLastAnswered() > 10000)
                            || (!p.isConnecting && p.getLastAnswered() > Settings.pingTimeout)) {

//                        System.out.println("reason: " + p.isConnecting + " " + p.isConnected() + " " + p.getLastAnswered());

                        if (p.isConnected() || p.isConnecting) {

//                            Log.put(Settings.pingTimeout + " sec timeout reached! " + p.ip, 10);

                            p.disconnect("timeout");
                            if (p.getNodeId() == null) {
                                Log.put("removed peer from peerList, tried once and peer never connected before: " + p.ip + ":" + p.port, 120);
                            }

                            //todo: interrupt outbound thread?
                        } else if (p.getLastAnswered() > Settings.pingTimeout * 2) {
                            p.writeBuffer = null;
                            p.writeBufferCrypted = null;
                        }

                    } else if (p.isConnected()) {

                        //                                System.out.println("Pinging: " + p.nonce);
                        //p.ping();
                        p.cnt++;
                        if (p.cnt > Settings.peerListRequestDelay * 1000 / (Settings.pingDelay)) {
                            p.ping();
                            p.cnt = 0;
                        } else {
                            if (p.isConnected() && p.getLastAnswered() > Settings.pingDelay) {
                                p.ping();
                            } else {
                            }


                        }
                    }


                }
            } finally {
                lock.unlock();
            }
        }
    }
}


