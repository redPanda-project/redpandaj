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

            serverContext.getConnectionHandler().getPeerInHandshakesLock().lock();
            try {
                long currentTimeMillis = System.currentTimeMillis();
                ArrayList<PeerInHandshake> toRemove = new ArrayList<>();
                for (PeerInHandshake peerInHandshake : ConnectionHandler.peerInHandshakes) {
                    if (currentTimeMillis - peerInHandshake.getCreatedAt() > 1000L * 10L) {
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
                serverContext.getConnectionHandler().getPeerInHandshakesLock().unlock();
            }

            Lock lock = peerList.getReadWriteLock().readLock();
            lock.lock();
            try {
                for (Peer peer : peerList.getPeerArrayList()) {

                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    Log.put("running over peer: " + peer, 120);

                    if ((peer.isConnecting && peer.getLastAnswered() > 10000)
                            || (!peer.isConnecting && peer.getLastAnswered() > Settings.pingTimeout)) {

                        if (peer.isConnected() || peer.isConnecting) {

                            peer.disconnect("timeout");
                            if (peer.getNodeId() == null) {
                                Log.put("removed peer from peerList, tried once and peer never connected before: "
                                        + peer.ip + ":" + peer.port, 120);
                            }

                            // todo: interrupt outbound thread?
                        } else if (peer.getLastAnswered() > Settings.pingTimeout * 2) {
                            peer.writeBuffer = null;
                            peer.writeBufferCrypted = null;
                        }

                    } else if (peer.isConnected()) {

                        peer.cnt++;
                        if (peer.cnt > Settings.peerListRequestDelay * 1000 / (Settings.pingDelay)) {
                            peer.sendPing();
                            peer.cnt = 0;
                        } else {
                            if (peer.isConnected() && peer.getLastAnswered() > Settings.pingDelay) {
                                peer.sendPing();
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
