package im.redpanda.core;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PeerJobs extends Thread {

    private static PeerJobs pj;

    static {
        pj = new PeerJobs();
    }


    public static void startUp() {
        pj.start();
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

        while (!Server.SHUTDOWN) {

            try {
                sleep(3000 + Server.secureRandom.nextInt(10000));
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }

            if (PeerList.size() == 0) {
                continue;
            }

            PeerList.getReadWriteLock().readLock().lock();
            try {
                for (Peer p : PeerList.getPeerArrayList()) {

                    Log.put("running over peer: " + p, 70);

//                    if (p.getLastAnswered() > 1000 * 60 * 60 * 24 * 7 && PeerList.size() > 3 && p.lastActionOnConnection != 0) {
//                        PeerList.remove(p);
//                    }

//                    if (p.lastPinged - p.lastActionOnConnection > Settings.pingDelay * 2 + 30000
//                            || (p.isConnecting && p.getLastAnswered() > 10000)
//                            || (!p.isConnected() && p.getLastAnswered() > Settings.pingDelay)) {

                    if ((p.isConnecting && p.getLastAnswered() > 10000)
                            || (!p.isConnected() && p.getLastAnswered() > Settings.pingTimeout)) {

                        System.out.println("" + p.isConnecting + " " + p.isConnected() + " " + p.getLastAnswered());

                        if (p.isConnected() || p.isConnecting) {

                            Log.put(Settings.pingTimeout + " sec timeout reached! " + p.ip, 10);

                            p.disconnect("timeout");
                            if (p.getNodeId() == null) {
                                Log.put("removed peer from peerList, tried once and peer never connected before: " + p.ip + ":" + p.port, 20);
                            }

                            //todo: interrupt outbound thread?
                        } else if (p.getLastAnswered() > Settings.pingTimeout * 1000 * 2) {
                            p.writeBuffer = null;
                            p.readBuffer = null;
                            p.readBufferCrypted = null;
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
                PeerList.getReadWriteLock().readLock().unlock();
            }
        }
    }
}


