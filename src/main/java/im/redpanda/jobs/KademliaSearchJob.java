package im.redpanda.jobs;


import im.redpanda.core.*;
import im.redpanda.kademlia.KadContent;
import im.redpanda.kademlia.PeerComparator;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class KademliaSearchJob extends Job {

    /**
     * Here we use a blacklist to block search request for the same KademliaId in short time intervals.
     * If a search is initialized from a node and the next nodes are also have to start a search request,
     * it is possible that two nodes request the same search from use.
     * This blacklist will block any duplicated request for the same search.
     */
    private static HashMap<KademliaId, Long> kademliaIdSearchBlacklist = new HashMap<KademliaId, Long>();
    private static ReentrantLock kademliaIdSearchBlacklistLock = new ReentrantLock();
    private static long BLACKLIST_KEY_FOR = 1000L * 5L;
    //todo: we need a housekeeper for this hashmap!

    public static final int SEND_TO_NODES = 2;
    private static final int NONE = 0;
    private static final int ASKED = 2;
    private static final int SUCCESS = 1;

    private KademliaId id;
    private TreeMap<Peer, Integer> peers = null;
    private ArrayList<KadContent> contents = new ArrayList<>();

    public KademliaSearchJob(KademliaId id) {
        this.id = id;
    }

    @Override
    public void init() {


        /**
         * Lets check if this KademliaId was already searched in the last seconds such that no search loops occur.
         * Each Key is blacklisted for 5 seconds after a search and only direct searches will be returned for that key.
         * TODO: Maybe we should create a list of "requesters" for each search such that we can send an answers to all "requesters".
         */

        long currentTimeMillis = System.currentTimeMillis();

        kademliaIdSearchBlacklistLock.lock();
        try {
            Long blacklistedTill = kademliaIdSearchBlacklist.get(id);
            if (blacklistedTill != null)
            System.out.println("tillasd: " + (currentTimeMillis - blacklistedTill));
            if (blacklistedTill == null || currentTimeMillis - blacklistedTill >= 0) {
                kademliaIdSearchBlacklist.put(id, currentTimeMillis + BLACKLIST_KEY_FOR);
            } else {
                System.out.println("search is blacklisted....");
                //todo: maybe we should inform the peer that he should retry a KadSearch in some seconds?
                return;
            }
        } finally {
            kademliaIdSearchBlacklistLock.unlock();
        }


        int myDistanceToKey = id.getDistance(Server.NONCE);

//        BigInteger key = id.getInt();
//        BigInteger me = Server.NONCE.getInt();
//        BigInteger myDistanceToKey = me.xor(key).abs();


        //key is not blacklisted, lets sort the peers by the destination key
        peers = new TreeMap<>(new PeerComparator(id));

        //insert all nodes
        PeerList.getReadWriteLock().readLock().lock();
        try {
            ArrayList<Peer> peerList = PeerList.getPeerArrayList();

            if (peerList == null) {
                initilized = false;
                return;
            }

            for (Peer p : peerList) {

                //do not add the peer if the peer is not connected or the nodeId is unknown!
                if (p.getNodeId() == null || !p.isConnected()) {
                    continue;
                }

                //do not ask light clients for kad entries...
                if (p.isLightClient()) {
                    continue;
                }

                /**
                 * do not add peers which are further or equally away from the key than us
                 */
                int peersDistanceToKey = id.getDistance(p.getKademliaId());
                System.out.println("my distance: " + myDistanceToKey + " theirs distance: " + peersDistanceToKey);
                if (myDistanceToKey >= peersDistanceToKey) {
                    continue;
                }


                peers.put(p, NONE);
            }
        } finally {
            PeerList.getReadWriteLock().readLock().unlock();
        }

        System.out.println("peers for search found: " + peers.size());
    }

    @Override
    public void work() {


        /**
         * check for timeout, maybe we already got an answer but not SEND_TO_NODES
         */
        if (getEstimatedRuntime() > 1000 * 5) {
            System.out.println("5 second timeout reached for KadSearch... ");
            success();
            done();
            return;
        }
        if (peers == null) {
            System.out.println("search blacklisted for KadSearch... ");
            fail();
            done();
            return;
        }

        int askedPeers = 0;
        int successfullPeers = 0;
        for (Peer p : peers.keySet()) {


            Integer status = peers.get(p);
            if (status == SUCCESS) {
                successfullPeers++;
                askedPeers++;
                continue;
            } else if (status == ASKED) {
                continue;
            }


            if (successfullPeers >= SEND_TO_NODES) {
                break;
            }


            if (askedPeers >= SEND_TO_NODES) {
                //the check for done will be made below the loop
                break;
            }


            if (p.isConnected() && p.isIntegrated()) {

                try {
                    //lets not wait too long for a lock, since this job may timeout otherwise
                    boolean lockedByMe = p.getWriteBufferLock().tryLock(50, TimeUnit.MILLISECONDS);
                    if (lockedByMe) {
                        try {

                            ByteBuffer writeBuffer = p.getWriteBuffer();

                            if (writeBuffer == null) {
                                continue;
                            }

                            peers.put(p, ASKED);
                            askedPeers++;


                            System.out.println("seach KadId from peer: " + p.getNodeId().toString() + " size: " + peers.size() + " distance: " + id.getDistance(p.getKademliaId()) + " target: " + id);


                            writeBuffer.put(Command.KADEMLIA_GET);
                            writeBuffer.putInt(getJobId());
                            writeBuffer.put(id.getBytes());

                            p.setWriteBufferFilled();

                        } finally {
                            p.getWriteBufferLock().unlock();
                        }
                    } else {

                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }


        }

        /**
         * Lets check if already SEND_TO_NODES peers answered and check if all peers list answered,
         * the peers list may be small if we are near the search key...
         */
        if (successfullPeers >= SEND_TO_NODES || successfullPeers == peers.size()) {
            success();
            done();
        }


    }


    protected void fail() {
    }


    protected ArrayList<KadContent> success() {

        System.out.println("sucess!!!222");

        if (contents.isEmpty()) {
            return null;
        }

        //lets get the newest one!
        contents.sort(new Comparator<KadContent>() {
            @Override
            public int compare(KadContent o1, KadContent o2) {
                return o1.getTimestamp() < o1.getTimestamp() ? -1 :
                        o1.getTimestamp() > o1.getTimestamp() ? 1 : 0;
            }
        });

        System.out.println("newst content found: " + contents.get(0).getTimestamp());

        return contents;
    }


    public void ack(KadContent c, Peer p) {
        //todo: concurrency?
        //todo: save KadContent in our store?
        contents.add(c);
        peers.put(p, SUCCESS);
        System.out.println("ack2!!");
    }


    public static HashMap<KademliaId, Long> getKademliaIdSearchBlacklist() {
        return kademliaIdSearchBlacklist;
    }

    public static ReentrantLock getKademliaIdSearchBlacklistLock() {
        return kademliaIdSearchBlacklistLock;
    }
}