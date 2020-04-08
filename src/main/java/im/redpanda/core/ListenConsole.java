package im.redpanda.core;

import im.redpanda.kademlia.KadStoreManager;
import im.redpanda.store.NodeStore;
import org.apache.commons.pool2.impl.DefaultPooledObjectInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ListenConsole extends Thread {

    @Override
    public void run() {
        try {
            listen();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void listen() throws IOException {

        InputStreamReader inputStreamReader = new InputStreamReader(System.in, "UTF-8");
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        while (!Server.SHUTDOWN) {

            String readLine = bufferedReader.readLine();

            if (PeerList.size() == 0) {
                continue;
            }

            if (readLine.equals("")) {

                System.out.println("Status listenPort: " + Server.MY_PORT + " NONCE: " + Server.NONCE + "\n");

                int actCons = 0;

                PeerList.getReadWriteLock().writeLock().lock();
                try {

                    ArrayList<Peer> peerArrayList = PeerList.getPeerArrayList();

                    ArrayList<Peer> list = peerArrayList;
//                    Collections.sort(peerTrustsCloned, new Comparator<PeerTrustData>() {
//
//                        @Override
//                        public int compare(PeerTrustData o1, PeerTrustData o2) {
//                            return (int) (o2.lastSeen - o1.lastSeen);
//                        }
//                    });
                    Collections.sort(list);

//                    System.out.println("IP:PORT \t\t\t\t\t\t Nonce \t\t\t Last Answer \t Alive \t retries \t LoadedMsgs \t Ping \t Authed \t PMSG\n");
                    System.out.format("%40s %18s %12s %12s %7s %8s %10s %10s %10s %8s %10s %10s %10s\n", "[IP]:PORT", "nonce", "last answer", "conntected", "retries", "ping", "loaded Msg", "bytes out", "bytes in", "bad Msg", "ToSyncM", "RSM", "Rating");
                    for (Peer peer : list) {

                        if (peer.isConnected()) {
                            actCons++;
                        }

                        //System.out.println("Peer: " + InetAddress.getByName(peer.ip) + ":" + peer.port + " Nonce: " + peer.nonce + " Last Answer: " + (System.currentTimeMillis() - peer.lastActionOnConnection) + " Alive: " + peer.isConnected() + " LastGetAllMsgs: " + peer.lastAllMsgsQuerried + " retries: " + peer.retries + " LoadedMsgs: " + peer.loadedMsgs + " ping: " + (Math.round(peer.ping * 100) / 100.));
                        String c;
                        if (peer.lastActionOnConnection != 0) {
                            c = "" + (System.currentTimeMillis() - peer.lastActionOnConnection);
                        } else {
                            c = "-";
                        }

                        String nodeId;

                        if (peer.getNodeId() == null) {
                            nodeId = "-";
                        } else {
                            nodeId = peer.getNodeId().getKademliaId().toString().substring(0, 10);
                        }

//                        while (c.length() < 15) {
//                            c += " \t";
//                        }

                        System.out.format("%40s %18s %12s %12s %7d %8s %10s %10d %10d %10d\n", "[" + peer.ip + "]:" + peer.port, nodeId, c, "" + peer.isConnected() + "/" + (peer.authed && peer.writeBufferCrypted != null), peer.retries, (Math.round(peer.ping * 100) / 100.), "-", peer.sendBytes, peer.receivedBytes, peer.removedSendMessages.size());


                    }


//                    System.out.format("%12s %25s %12s %12s\n", "ID", "Last Seen", "SyncedMsgs", "ToSync");


                    System.out.println("Connected to " + actCons + " peers. (NAT type: " + (Settings.NAT_OPEN ? "open" : "closed") + ")");
                    System.out.println("Traffic: " + Server.inBytes / 1024. + " kb / " + Server.outBytes / 1024. + " kb.");

//                    System.out.println("Services last run: ConnectionHandler: " + (System.currentTimeMillis() - ConnectionHandler.lastRun) + " MessageDownloader: " + (System.currentTimeMillis() - MessageDownloader.lastRun) + " MessageVerifierHsqlDb: " + (System.currentTimeMillis() - MessageVerifierHsqlDb.lastRun));
//                    System.out.println("Livetime socketio connections: " + Stats.getSocketioConnectionsLiveTime());

                    int size = 0;
                    ConnectionReaderThread.threadLock.lock();
                    try {
                        size = ConnectionReaderThread.threads.size();
                    } finally {
                        ConnectionReaderThread.threadLock.unlock();
                    }


                    System.out.println("Threads: " + size);

                    Map<String, List<DefaultPooledObjectInfo>> stringListMap = ByteBufferPool.getPool().listAllObjects();

                    String out = "";

                    for (String s : stringListMap.keySet()) {
                        out += "key: " + s + " size: " + stringListMap.get(s).size() + "\n";
                    }


//                    System.out.println("\n\nList of ByteBufferPool: \n" + out + "\n\n");


                    System.out.println("KadStore entries: ");
                    KadStoreManager.printStatus();


                } finally {
                    PeerList.getReadWriteLock().writeLock().unlock();
                }
            } else if (readLine.equals("ll")) {
                System.out.println("New Log Level:");
                String readLine2 = bufferedReader.readLine();
                try {
                    Log.LEVEL = Integer.parseInt(readLine2);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            } else if (readLine.equals("e")) {
                Server.nodeStore.saveToDisk();
                Server.shutdown();
                System.exit(0);
            } else if (readLine.equals("c")) {
                System.out.println("closing all connections...");

                PeerList.getReadWriteLock().writeLock().lock();
                for (Peer peer : PeerList.getPeerArrayList()) {
                    peer.disconnect("disconnect by user");
                }
                PeerList.getReadWriteLock().writeLock().unlock();


            } else if (readLine.equals("alloc")) {
                System.out.println("allocating buffers by pool");

                ByteBufferPool.returnObject(ByteBufferPool.borrowObject(1024 * 1024 * 4));


            } else if (readLine.equals("a")) {
                System.out.println("add ip:port");


                String readLine2 = bufferedReader.readLine();
                String[] split = readLine2.split(":");
                try {
                    int port = Integer.parseInt(split[1]);
                    PeerList.add(new Peer(split[0], port));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }

            }
        }
    }

}
