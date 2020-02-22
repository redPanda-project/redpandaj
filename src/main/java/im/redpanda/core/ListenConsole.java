package im.redpanda.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;

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

        while (!Server.SHUTDOWN) {
            InputStreamReader inputStreamReader = new InputStreamReader(System.in, "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String readLine = bufferedReader.readLine();

            if (PeerList.size() == 0) {
                continue;
            }

            if (readLine.equals("")) {

                System.out.println("Status listenPort: " + Server.MY_PORT + " NONCE: " + Server.NONCE + "\n");

                int actCons = 0;

                PeerList.getReadWriteLock().readLock().lock();
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
                    System.out.format("%50s %22s %12s %12s %7s %8s %10s %10s %10s %8s %10s %10s %10s\n", "[IP]:PORT", "nonce", "last answer", "conntected", "retries", "ping", "loaded Msg", "bytes out", "bytes in", "bad Msg", "ToSyncM", "RSM", "Rating");
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

                        while (c.length() < 15) {
                            c += " \t";
                        }

                        System.out.format("%50s %22s %12s %12s %7d %8s %10s %10d %10d %10d\n", "[" + peer.ip + "]:" + peer.port, nodeId, c, "" + peer.isConnected() + "/" + (peer.authed && peer.writeBufferCrypted != null), peer.retries, (Math.round(peer.ping * 100) / 100.), "-", peer.sendBytes, peer.receivedBytes, peer.removedSendMessages.size());


                    }


//                    System.out.format("%12s %25s %12s %12s\n", "ID", "Last Seen", "SyncedMsgs", "ToSync");


                    System.out.println("Connected to " + actCons + " peers. (NAT type: " + (Settings.NAT_OPEN ? "open" : "closed") + ")");
                    System.out.println("Traffic: " + Server.inBytes / 1024. + " kb / " + Server.outBytes / 1024. + " kb.");

//                    System.out.println("Services last run: ConnectionHandler: " + (System.currentTimeMillis() - ConnectionHandler.lastRun) + " MessageDownloader: " + (System.currentTimeMillis() - MessageDownloader.lastRun) + " MessageVerifierHsqlDb: " + (System.currentTimeMillis() - MessageVerifierHsqlDb.lastRun));
//                    System.out.println("Livetime socketio connections: " + Stats.getSocketioConnectionsLiveTime());

                } finally {
                    PeerList.getReadWriteLock().readLock().unlock();
                }
            }
        }
    }

}
