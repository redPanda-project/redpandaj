package im.redpanda.jobs;



import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;

import java.util.ArrayList;
import java.util.TreeMap;

public class KademliaPeerLookupJob extends Job {


    private KademliaId destination;
    private TreeMap<Peer, Integer> peers = new TreeMap<>();


    public KademliaPeerLookupJob(KademliaId destination) {
        this.destination = destination;


        //insert all nodes
        PeerList.getReadWriteLock().readLock().lock();
        try {
            ArrayList<Peer> peerList = PeerList.getPeerArrayList();

            for (Peer p : peerList) {
                peers.put(p, 0);
            }

        } finally {
            PeerList.getReadWriteLock().readLock().unlock();
        }

    }

    @Override
    public void init() {

    }

    @Override
    public void work() {

        for (Peer p : peers.keySet()) {


            if (!p.isConnected() && !p.isConnecting) {

            }


        }


    }


}
