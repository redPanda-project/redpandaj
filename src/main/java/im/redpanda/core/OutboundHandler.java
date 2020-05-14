package im.redpanda.core;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;

public class OutboundHandler extends Thread {

    long lastAddedKnownNodes;
    Random random = new Random();

    public void init() {
        start();


        if (PeerList.getPeerArrayList() == null || Server.NONCE == null) {
            throw new RuntimeException("Do not start the outbound thread if peers and nonce not loaded!");
        }
    }


    boolean allowInterrupt = false;

    public void tryInterrupt() {
//        System.out.println("try interrupt");
        if (allowInterrupt) {
            //todo maybe reactivate later...
//            interrupt();
        }
    }


    @Override
    public void run() {

        final String orgName = Thread.currentThread().getName();
        Thread.currentThread().setName(orgName + " - OutboundThread");


        ReadWriteLock peerListLock = Server.peerListLock;
        ArrayList<Peer> peerList = PeerList.getPeerArrayList();

        ArrayList<Peer> peersToRemove = new ArrayList<>();

        long loopCount = 0;

        while (!Server.SHUTDOWN) {

//            System.out.println("Peers: " + PeerList.size());


            if (PeerList.size() < 5) {
                reseed();
            }

            try {
                peerListLock.writeLock().lock();
                Collections.sort(peerList);
            } catch (java.lang.IllegalArgumentException e) {
                try {
                    sleep(200);
                } catch (InterruptedException ex) {
                    Log.putCritical(ex);
                }
                continue;
            } finally {
                peerListLock.writeLock().unlock();
            }


            peerListLock.readLock().lock();
            try {
                int actCons = 0;
                int connectingCons = 0;
                for (Peer peer : peerList) {
                    if (peer.isConnected()) {
                        actCons++;
                    } else if (peer.isConnecting) {
                        connectingCons++;
                    }

//                    if ((peer.isConnecting || peer.isConnected()) && (System.currentTimeMillis() - peer.lastActionOnConnection > 30000)) {
//                        peer.disconnect("timeout ...");
//                    }

                }

                actCons += connectingCons;
                int cnt = 0;
                for (Peer peer : peerList) {


                    cnt++;

                    if (actCons >= Settings.MIN_CONNECTIONS) {

                        Log.put("peers " + actCons + " are enough...", 300);

                        if (cnt == 1 && actCons >= Settings.MAX_CONNECTIONS) {
                            for (Peer p1 : peerList) {
                                if (p1.isConnected()) {
                                    p1.disconnect("max cons");

                                    System.out.println("closed one connection...");

                                    break;
                                }
                            }
                        }
                        break;
                    }

                    if (peer.port == 0) {
                        continue;
                    }

                    if (peer.isConnected()) {
                        continue;
                    }

                    boolean alreadyConnectedToSameIpandPort = false;
                    for (Peer p2 : peerList) {
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
                        //already connected to same trusted node?
                        for (Peer p2 : peerList) {

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
//                        peer.disconnect();
//                        if (DEBUG) {
//                            System.out.println("closing con, cuz i wanna connect...");
//                        }
                    }

//                    if (peerList.size() > 20) {
                    //(System.currentTimeMillis() - peer.lastActionOnConnection > 1000 * 60 * 60 * 4)
                    if ((peer.retries > 10 || (peer.getKademliaId() == null && peer.retries >= 5)) && peer.ping != -1) {
                        //peerList.remove(peer);
                        peersToRemove.add(peer);

                        if (peer.retries < 200) {

//                            Test.messageStore.insertPeerConnectionInformation(peer.ip, peer.port, 0, 0);
//                        Test.messageStore.setStatusForPeerConnectionInformation(peer.ip, peer.port, peer.retries, System.currentTimeMillis() + 1000L * 60L * peer.retries);
//                            Test.messageStore.setStatusForPeerConnectionInformation(peer.ip, peer.port, peer.retries, System.currentTimeMillis() + 1000L * 60L * 5L);

                            Log.put("removed peer from peerList, too many retries: " + peer.ip + ":" + peer.port, 20);

                        } else {
                            //we do not have to remove peers here because every peer in peerlist should not be in the db!

                            Log.put("removed peer permanently, too many retries: " + peer.ip + ":" + peer.port, 20);

                        }

                        continue;
                    }
                    peer.ping = 0;


                    if (peer.connectAble != -1) {

                        Log.put("try to connect to new node: " + peer.ip + ":" + peer.port, 150);

                        connectTo(peer);
                        actCons++;
                        try {
                            sleep(200);
                        } catch (InterruptedException ex) {
                        }

                    } else {
                        System.out.println("connect state: " + peer.connectAble + " -- " + peer.ip + ":" + peer.port);
                    }


                }
            } finally {
                peerListLock.readLock().unlock();
            }


            for (Peer toRemove : peersToRemove) {
//                System.out.println("removing peer from OH: " + toRemove.getKademliaId());
                Server.removePeer(toRemove);
            }
            peersToRemove.clear();

            try {
                allowInterrupt = true;

                sleep(1000 + random.nextInt(3000));

            } catch (InterruptedException ex) {
            } finally {
                allowInterrupt = false;
            }

        }
    }

    private void reseed() {

        if (System.currentTimeMillis() - lastAddedKnownNodes < 1000L * 60L * 10L) {
            return;
        }

        lastAddedKnownNodes = System.currentTimeMillis();


        Server.peerListLock.writeLock().lock();
        try {
            for (String hostport : Settings.knownNodes) {
                String[] split = hostport.split(":");
                String host = split[0];
                int port = Integer.parseInt(split[1]);


//                Server.findPeer(new Peer(host, port));
                PeerList.add(new Peer(host, port));

            }
        } finally {
            Server.peerListLock.writeLock().unlock();
        }

    }


    private static void connectTo(final Peer peer) {

        peer.retries++;
        peer.isConnecting = true;
        peer.isConnectionInitializedByMe = true;
        peer.lastActionOnConnection = System.currentTimeMillis();

        Node byKademliaId = Node.getByKademliaId(peer.getKademliaId());

        int retries = 0;
        if (byKademliaId != null) {
            retries = byKademliaId.incrRetry(peer.getIp(), peer.getPort());
            peer.retries = retries;
            //todo if retries to high disconnect?
        }


        try {
            SocketChannel open = SocketChannel.open();
            open.configureBlocking(false);

            boolean alreadyConnected = open.connect(new InetSocketAddress(peer.ip, peer.port));

            PeerInHandshake peerInHandshake = new PeerInHandshake(peer.ip, peer, open);
            ConnectionHandler.peerInHandshakesLock.lock();
            try {
                ConnectionHandler.peerInHandshakes.add(peerInHandshake);
            } finally {
                ConnectionHandler.peerInHandshakesLock.unlock();
            }

            /**
             * Lets check if we have a nodeId and add it to the PeerInHandShake
             */
            if (peer.getNodeId() != null) {
                peerInHandshake.setNodeId(peer.getNodeId());
            }

            peerInHandshake.addConnection(alreadyConnected);

        } catch (UnknownHostException ex) {
            System.out.println("outgoing con failed, unknown host...");
        } catch (Exception ex) {
            ex.printStackTrace();
            Log.put("outgoing con failed... " + peer.ip, 0);
        }
    }


}
