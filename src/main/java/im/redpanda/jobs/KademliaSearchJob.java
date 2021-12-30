package im.redpanda.jobs;


import im.redpanda.core.Command;
import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.core.ServerContext;
import im.redpanda.kademlia.KadContent;
import im.redpanda.kademlia.PeerComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KademliaSearchJob extends Job {

    /**
     * Here we use a blacklist to block search request for the same KademliaId in short time intervals.
     * If a search is initialized from a node and the next nodes also have to start a search request,
     * it is possible that two nodes request the same search from us.
     * This blacklist will block any duplicated request for the same search.
     */
    private static final HashMap<KademliaId, Long> kademliaIdSearchBlacklist = new HashMap<KademliaId, Long>();
    private static final ReentrantLock kademliaIdSearchBlacklistLock = new ReentrantLock();
    private static final long BLACKLIST_KEY_FOR = 1000L * 30L;
    //todo: we need a housekeeper for this hashmap!

    public static final int SEND_TO_NODES = 2;
    private static final int NONE = 0;
    private static final int ASKED = 2;
    private static final int SUCCESS = 1;

    private final KademliaId id;
    private TreeMap<Peer, Integer> peers = null;
    private final ArrayList<KadContent> contents = new ArrayList<>();

    public KademliaSearchJob(ServerContext serverContext, KademliaId id) {
        super(serverContext);
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
            if (blacklistedTill == null || currentTimeMillis - blacklistedTill >= 0) {
                kademliaIdSearchBlacklist.put(id, currentTimeMillis + BLACKLIST_KEY_FOR);
            } else {
                //todo: maybe we should inform the peer that he should retry a KadSearch in some seconds?
                fail();
                done();
                return;
            }
        } finally {
            kademliaIdSearchBlacklistLock.unlock();
        }


        int myDistanceToKey = id.getDistance(serverContext.getNonce());

//        BigInteger key = id.getInt();
//        BigInteger me = Server.NONCE.getInt();
//        BigInteger myDistanceToKey = me.xor(key).abs();


        //key is not blacklisted, lets sort the peers by the destination key
        peers = new TreeMap<>(new PeerComparator(id));

        PeerList peerList = serverContext.getPeerList();

        //insert all nodes
        Lock lock = peerList.getReadWriteLock().readLock();
        lock.lock();
        try {
            ArrayList<Peer> peerArrayList = peerList.getPeerArrayList();

            if (peerArrayList == null) {
                initilized = false;
                return;
            }

            for (Peer p : peerArrayList) {

                //do not add the peer if the peer is not connected or the nodeId is unknown!
                if (p.getNodeId() == null || !p.isConnected()) {
                    continue;
                }

                //do not ask light clients for kad entries...
                if (p.isLightClient()) {
                    continue;
                }

//                /**
//                 * do not add peers which are further or equally away from the key than us
//                 */
//                int peersDistanceToKey = id.getDistance(p.getKademliaId());
//                System.out.println("my distance: " + myDistanceToKey + " theirs distance: " + peersDistanceToKey);
//                if (myDistanceToKey <= peersDistanceToKey) {
//                    continue;
//                }


                peers.put(p, NONE);
            }
        } finally {
            lock.unlock();
        }

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

        return contents;
    }


    public void ack(KadContent c, Peer p) {
        //todo: concurrency?
        contents.add(c);
        peers.put(p, SUCCESS);
    }


    public static HashMap<KademliaId, Long> getKademliaIdSearchBlacklist() {
        return kademliaIdSearchBlacklist;
    }

    public static ReentrantLock getKademliaIdSearchBlacklistLock() {
        return kademliaIdSearchBlacklistLock;
    }
}
