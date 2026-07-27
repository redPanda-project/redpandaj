package im.redpanda.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;

public class PeerJobs extends Thread {

  /** A handshake that has not completed within this time is closed and dropped. */
  static final long HANDSHAKE_TIMEOUT_MS = 1000L * 10L;

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

    while (!Server.isShuttingDown()) {

      try {
        sleep(1000 + Server.secureRandom.nextInt(4000));
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }

      runOnce();
    }
  }

  /**
   * One pass of the chron loop. Package-private so the ordering below can be tested without
   * starting the thread and waiting out its sleeps.
   */
  void runOnce() {
    // Stale handshakes are reaped independently of the peer list: a handshake that never
    // completes is not in the peer list yet, so gating this on `peerList.size() != 0` left
    // those channels open forever whenever the node had no established peers (fresh start,
    // total connection loss) — exactly the situation in which handshakes pile up.
    reapStaleHandshakes();

    if (peerList.size() == 0) {
      return;
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
              Log.put(
                  "removed peer from peerList, tried once and peer never connected before: "
                      + peer.ip
                      + ":"
                      + peer.port,
                  120);
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

  /**
   * Closes and drops every {@link PeerInHandshake} that has not completed within {@link
   * #HANDSHAKE_TIMEOUT_MS}. Package-private so the timeout behaviour is testable without starting
   * the thread.
   */
  void reapStaleHandshakes() {
    serverContext.getConnectionHandler().getPeerInHandshakesLock().lock();
    try {
      long currentTimeMillis = System.currentTimeMillis();
      ArrayList<PeerInHandshake> toRemove = new ArrayList<>();
      for (PeerInHandshake peerInHandshake : ConnectionHandler.peerInHandshakes) {
        if (currentTimeMillis - peerInHandshake.getCreatedAt() > HANDSHAKE_TIMEOUT_MS) {
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
  }
}
