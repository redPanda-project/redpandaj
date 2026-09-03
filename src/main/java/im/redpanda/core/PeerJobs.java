package im.redpanda.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    // TD026: snapshot the list under the read lock, then iterate WITHOUT holding it. The loop
    // below sleeps 20 ms per peer, so holding the read lock across it pinned the peer list for
    // size() * 20 ms — with the couple of hundred peers a seed node accumulates that is several
    // seconds of continuously held read lock. Note the cost is linear in the list size, so this
    // was harmless while lists were small and only became visible as they grew.
    //
    // A ReentrantReadWriteLock queues writers behind that, and since #280 made PeerList.add()
    // always take the write lock, ConnectionHandler.setupConnection() — which runs on the SELECTOR
    // thread — blocked there for seconds. That stalls the entire NIO event loop: no reads, no
    // writes, for every peer at once. Waiting readers are blocked too (a queued writer makes new
    // readers block), so InboundCommandProcessor.handlePing()'s peerList.contains() on the reader
    // threads waited hundreds of milliseconds, with multi-second outliers, before it could even
    // queue the PONG reply. Net effect: peer ping went from the true ~20 ms network RTT to
    // hundreds of ms. The measured figures behind this are in the PR description and in TD026
    // rather than here, so they cannot drift out of sync with the code.
    //
    // The copy keeps the iteration CME-safe exactly as the held lock did; nothing in the loop
    // touches the peer list itself, only the Peer objects, which the peer list lock never guarded.
    List<Peer> peers = peerList.snapshot();

    evictUndialableDisconnectedPeers(peers);

    for (Peer peer : peers) {

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
          releaseWriteBuffers(peer);
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
  }

  /**
   * Frees the 2 × 300 KiB write buffers of a peer that has been silent for twice the ping timeout.
   *
   * <p>T115: the reap itself, including the lock it must be done under and the re-test of the
   * condition inside that lock, now lives in {@link Peer#releaseWriteBuffersIfIdle()} — the class
   * that owns those buffers. TD029 (REDPANDAJ-2EJ) was exactly this code running without that lock,
   * so keeping the discipline next to the fields rather than at this call site is the point.
   *
   * <p>Lock order (documented on {@link PeerList}): {@code writeBufferLock} is the outermost of the
   * three, and {@link #runOnce()} released the peer list read lock right after taking its snapshot,
   * so nothing can be inverted here — the same loop already calls {@link Peer#disconnect(String)},
   * which takes exactly this lock.
   */
  private static void releaseWriteBuffers(Peer peer) {
    peer.releaseWriteBuffersIfIdle();
  }

  /**
   * Drops peers that are neither connected nor dialable — the retention leak behind the ~280-entry
   * peer lists (T86).
   *
   * <p>Every inbound connection puts a {@link Peer} into the peer list, built from the remote end
   * of the socket plus the listening port the handshake announced. A light client has no listening
   * socket and announces port 0, so its entry is undialable from the moment it is created and dead
   * weight from the moment it disconnects. Nothing ever removed those again: the only eviction
   * path, {@code OutboundHandler}, skips undialable peers <em>before</em> it reaches its
   * retry-based removal, so their {@code retries} counter never moves. They then get written to
   * {@code peers.dat}, restored on the next start and gossiped on to every other node. Measured on
   * a node bootstrapped from the testnet seeds: 273 of 278 entries had port 0, each with a distinct
   * identity — one per mobile app instance, per re-install and per e2e run.
   *
   * <p>A still-connected peer is kept regardless: that is the live inbound connection, and it is
   * the one thing such an entry is good for. The entry is recreated by the next handshake, which is
   * how it came about in the first place.
   */
  private void evictUndialableDisconnectedPeers(List<Peer> peers) {
    for (Peer peer : peers) {
      if (peer.isDialable() || peer.isConnected() || peer.isConnecting) {
        continue;
      }
      if (peerList.remove(peer)) {
        Log.put("removed undialable disconnected peer from peerList: " + peer, 40);
      }
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
