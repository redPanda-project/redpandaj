package im.redpanda.jobs;


import im.redpanda.core.*;
import im.redpanda.crypt.Utils;
import im.redpanda.kademlia.KadContent;
import im.redpanda.kademlia.PeerComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class KademliaInsertJob extends Job {

    public static final int SEND_TO_NODES = 2;
    private static final int NONE = 0;
    private static final int ASKED = 2;
    private static final int SUCCESS = 1;

    private final KadContent kadContent;
    private TreeMap<Peer, Integer> peers = null;


    public KademliaInsertJob(ServerContext serverContext, KadContent kadContent) {
        super(serverContext);
        this.kadContent = kadContent;
    }

    @Override
    public void init() {


        PeerList peerList = serverContext.getPeerList();

        //We first save the KadContent in our StoreManager, we use "dht-caching"
        // such that too far away entries will be removed faster
        serverContext.getKadStoreManager().put(kadContent);

        //lets sort the peers by the destination key
        peers = new TreeMap<>(new PeerComparator(kadContent.getId()));

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

                if (p.getNodeId() == null) {
                    continue;
                }

                peers.put(p, NONE);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void work() {


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
                done();
                break;
            }


            if (askedPeers >= SEND_TO_NODES) {
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


                            System.out.println("putKadCmd to peer: " + p.getNodeId().toString() + " size: " + peers.size() + " distance: " + kadContent.getId().getDistance(p.getKademliaId()) + " target: " + kadContent.getId());

                            int toWriteBytes = writeBuffer.position() + kadContent.getContent().length + 1024;

//                            if (p.writeBuffer.remaining() < toWriteBytes) {
//                                ByteBuffer allocate = ByteBuffer.allocate(toWriteBytes);
//                                p.writeBuffer.flip();
//                                allocate.put(p.writeBuffer);
//                                p.writeBuffer = allocate;
//                                writeBuffer = allocate;
//                            }


                            writeBuffer.put(Command.KADEMLIA_STORE);

                            writeBuffer.putInt(4 + 8 + NodeId.PUBLIC_KEYLEN + 4 + kadContent.getContent().length + 4 + kadContent.getSignature().length);

                            writeBuffer.putInt(getJobId());
//                            writeBuffer.put(kadContent.getId().getBytes());
                            writeBuffer.putLong(kadContent.getTimestamp());
                            writeBuffer.put(kadContent.getPubkey());

                            writeBuffer.putInt(kadContent.getContent().length);
                            writeBuffer.put(kadContent.getContent());


                            writeBuffer.putInt(kadContent.getSignature().length);
                            writeBuffer.put(kadContent.getSignature());

                            //for debug only
                            ByteBuffer allocate = ByteBuffer.allocate(kadContent.getSignature().length);
                            allocate.put(kadContent.getSignature());
                            allocate.flip();
                            byte[] bytes = Utils.readSignature(allocate);
                            if (bytes.length != kadContent.getSignature().length) {
                                throw new RuntimeException("could not read own signature......" + bytes.length);
                            }
                            ////////


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

//        System.out.println("successfullPeers: " + successfullPeers + " askedPeers: " + askedPeers);
        if (successfullPeers >= SEND_TO_NODES) {
            done();
        }


    }


    public void ack(Peer p) {
        //todo: concurrency?
        peers.put(p, SUCCESS);
    }


}
