package im.redpanda.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

public class OutboundHandler extends Thread {

  long lastAddedKnownNodes;
  SecureRandom random = new SecureRandom();
  private final PeerList peerList;
  private final ServerContext serverContext;

  public OutboundHandler(ServerContext serverContext) {
    this.peerList = serverContext.getPeerList();
    this.serverContext = serverContext;
  }

  boolean allowInterrupt = false;

  public void tryInterrupt() {
    // System.out.println("try interrupt");
  }

  private static boolean connectTo(ServerContext serverContext, final Peer peer) {

    peer.retries++;
    peer.isConnecting = true;
    peer.isConnectionInitializedByMe = true;
    peer.setLastPongReceived(System.currentTimeMillis());

    Node byKademliaId = Node.getByKademliaId(serverContext, peer.getKademliaId());

    int retries = 0;
    if (byKademliaId != null) {
      retries = byKademliaId.incrRetry(peer.getIp(), peer.getPort());
      peer.retries = retries;
      // todo if retries to high disconnect?
    }

    // The channel is owned by this method until the PeerInHandshake has taken it over; anything
    // that throws in between (unresolvable/invalid address, immediate connection refusal,
    // SecurityException, ...) used to leak the freshly opened SocketChannel, one file descriptor
    // per attempt. run() retries dead peers continuously, so this accumulated until exhaustion.
    SocketChannel open = null;
    boolean handedOver = false;
    try {
      open = SocketChannel.open();
      open.configureBlocking(false);

      boolean alreadyConnected = open.connect(new InetSocketAddress(peer.ip, peer.port));

      PeerInHandshake peerInHandshake = new PeerInHandshake(peer.ip, peer, open);
      serverContext.getConnectionHandler().addPeerInHandshake(peerInHandshake);
      // from here on the channel is closed by PeerInHandshake / Peer.disconnect()
      handedOver = true;

      /** Lets check if we have a nodeId and add it to the PeerInHandShake */
      if (peer.getNodeId() != null) {
        peerInHandshake.setNodeId(peer.getNodeId());
      }

      // addConnection() closes the channel and disconnects the peer when the selector
      // registration fails; reporting success in that case would suppress run()'s
      // `newConnections += 5` backoff, so the attempt is paced like any other failure.
      return peerInHandshake.addConnection(alreadyConnected);
    } catch (UnknownHostException ex) {
      System.out.println("outgoing con failed, unknown host...");
    } catch (Exception ex) {
      ex.printStackTrace();
      Log.put("outgoing con failed... " + peer.ip, 0);
    } finally {
      if (!handedOver && open != null) {
        try {
          open.close();
        } catch (IOException closeEx) {
          closeEx.printStackTrace();
        }
      }
    }
    return false;
  }

  private void reseed() {

    if (System.currentTimeMillis() - lastAddedKnownNodes < 1000L * 60L * 10L) {
      return;
    }

    lastAddedKnownNodes = System.currentTimeMillis();

    // No lock around the loop (T115): PeerList.add() takes the write lock itself and is atomic
    // per peer, which is all this needs — the seeds are independent and a concurrent add of the
    // same address is handled by add()'s own duplicate check.
    for (String hostport : Settings.knownNodes) {
      if (hostport.contains("[")) {
        // todo add port
        String[] split = hostport.split("]");
        String ipv6 = split[0].substring(1);
        peerList.add(new Peer(ipv6, 59558));
        continue;
      }

      String[] split = hostport.split(":");
      String host = split[0];
      int port = Integer.parseInt(split[1]);

      peerList.add(new Peer(host, port));
    }
  }

  @Override
  public void run() {

    final String orgName = getName();
    setName(orgName + " - OutboundThread");

    ArrayList<Peer> peersToRemove = new ArrayList<>();

    while (!Server.isShuttingDown()) {

      // System.out.println("Peers: " + PeerList.size());

      if (peerList.size() < 5) {
        reseed();
      }

      try {
        peerList.sortByPriority();
      } catch (IllegalArgumentException e) {
        // "Comparison method violates its general contract": a peer's priority depends on mutable
        // state, so a concurrent change can make TimSort bail out. Skip this round.
        try {
          sleep(200);
        } catch (InterruptedException ex) {
          Log.putCritical(ex);
        }
        continue;
      }

      // T87: snapshot under the read lock, then iterate WITHOUT holding it. The loop below calls
      // connectTo() (socket open + connect, and peer.disconnect() when the selector registration
      // fails) and peer.disconnect("max cons") — both take a peer's writeBufferLock. Holding the
      // peer list read lock across that inverts the documented lock order (see PeerList): the
      // selector thread holds a peer's writeBufferLock in ConnectionHandler.setupConnection() and
      // then takes the peer list WRITE lock, so the two could deadlock — and a wedged selector
      // takes the whole node down. Independently of that, this loop did network I/O under the read
      // lock, which starves the selector's writer for as long as a connect attempt takes.
      //
      // Same reasoning and same shape as TD026 in PeerJobs.runOnce(): the copy keeps the iteration
      // CME-safe exactly as the held lock did, and nothing in the loop touches the peer list
      // itself, only the Peer objects, which this lock never guarded.
      List<Peer> peers = peerList.snapshot();

      int actCons = 0;
      int connectingCons = 0;
      int newConnections = 0;
      for (Peer peer : peers) {
        if (peer.isConnected()) {
          actCons++;
        } else if (peer.isConnecting) {
          connectingCons++;
        }

        // if ((peer.isConnecting || peer.isConnected()) && (System.currentTimeMillis()
        // - peer.lastActionOnConnection > 30000)) {
        // peer.disconnect("timeout ...");
        // }

      }

      actCons += connectingCons;
      int cnt = 0;
      for (Peer peer : peers) {

        cnt++;

        if (newConnections >= 10) {
          break;
        }

        if (actCons >= Settings.MIN_CONNECTIONS) {

          Log.put("peers " + actCons + " are enough...", 300);

          if (cnt == 1 && actCons >= Settings.MAX_CONNECTIONS) {
            for (Peer p1 : peers) {
              if (p1.isConnected()) {
                p1.disconnect("max cons");

                System.out.println("closed one connection...");

                break;
              }
            }
          }
          break;
        }

        if (!peer.isDialable()) {
          // Nothing to connect to. Note this skip happens before the retry-based removal below,
          // so such a peer could never be evicted here - PeerJobs does it instead (T86).
          continue;
        }

        if (peerList.isBlacklisted(peer.getIp())) {
          continue;
        }

        if (peer.isConnected()) {
          continue;
        }

        boolean alreadyConnectedToSameIpandPort = false;
        for (Peer p2 : peers) {
          if (peer.equalsIpAndPort(p2) && (peer.isConnected() || peer.isConnecting)) {
            alreadyConnectedToSameIpandPort = true;
            break;
          }
        }

        if (alreadyConnectedToSameIpandPort) {
          continue;
        }

        if (Settings.IPV6_ONLY && peer.ip.length() <= 15) {
          peersToRemove.add(peer);
          Log.put("removed peer from peerList, no ipv6 address: " + peer.ip + ":" + peer.port, 200);
          continue;
        }

        if (Settings.IPV4_ONLY && peer.ip.length() > 15) {
          peersToRemove.add(peer);
          Log.put("removed peer from peerList, no ipv4 address: " + peer.ip + ":" + peer.port, 200);
          continue;
        }

        boolean alreadyConnectedToSameNodeId = false;
        if (peer.getKademliaId() != null) {
          // already connected to same trusted node?
          for (Peer p2 : peers) {

            if (alreadyConnectedToSameNodeId) {
              break;
            }

            if (!p2.isConnected() && !p2.isConnecting) {
              continue;
            }

            if (peer.equalsNonce(p2)) {
              alreadyConnectedToSameNodeId = true;
              break;
            }
          }
        }

        if (alreadyConnectedToSameNodeId) {
          Log.put("Do not connect to this peer, already connected to same KadId...", 70);
          continue;
        }

        if (peer.isConnected() || peer.isConnecting) {
          continue;
          // peer.disconnect();
          // if (DEBUG) {
          // System.out.println("closing con, cuz i wanna connect...");
          // }
        }

        // if (peerList.size() > 20) {
        // (System.currentTimeMillis() - peer.lastActionOnConnection > 1000 * 60 * 60 *
        // 4)
        if ((peer.retries > 10 || (peer.getKademliaId() == null && peer.retries >= 5))
            && peer.ping != -1) {
          // peerList.remove(peer);
          peersToRemove.add(peer);

          if (peer.retries < 200) {

            // Test.messageStore.insertPeerConnectionInformation(peer.ip, peer.port, 0, 0);
            // Test.messageStore.setStatusForPeerConnectionInformation(peer.ip, peer.port,
            // peer.retries, System.currentTimeMillis() + 1000L * 60L * peer.retries);
            // Test.messageStore.setStatusForPeerConnectionInformation(peer.ip, peer.port,
            // peer.retries, System.currentTimeMillis() + 1000L * 60L * 5L);

            Log.put(
                "removed peer from peerList, too many retries: " + peer.ip + ":" + peer.port, 20);

          } else {
            // we do not have to remove peers here because every peer in peerlist should not
            // be in the db!

            Log.put("removed peer permanently, too many retries: " + peer.ip + ":" + peer.port, 20);
          }

          continue;
        }
        peer.ping = 0;

        if (peer.connectAble != -1) {

          Log.put("try to connect to new node: " + peer.ip + ":" + peer.port, 150);

          boolean success = connectTo(serverContext, peer);
          actCons++;
          newConnections++;
          if (!success) {
            // if the connect method was not successful we had to wait longer than normally,
            // thus we should release the peerlist lock sooner...
            newConnections += 5;
          }
          // try {
          // sleep(200);
          // } catch (InterruptedException ex) {
          // }

        } else {
          System.out.println(
              "connect state: " + peer.connectAble + " -- " + peer.ip + ":" + peer.port);
        }
      }

      for (Peer toRemove : peersToRemove) {
        // System.out.println("removing peer from OH: " + toRemove.getKademliaId());
        peerList.remove(toRemove);
      }
      peersToRemove.clear();

      try {
        allowInterrupt = true;

        sleep(1000 + random.nextInt(3000));

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      } finally {
        allowInterrupt = false;
      }
    }
  }
}
