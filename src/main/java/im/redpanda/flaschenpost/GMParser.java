package im.redpanda.flaschenpost;

import im.redpanda.core.Command;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.core.Server;
import im.redpanda.jobs.Job;
import im.redpanda.jobs.PeerPerformanceTestFlaschenpostJob;
import im.redpanda.jobs.PeerPerformanceTestGarlicMessageJob;
import im.redpanda.kademlia.PeerComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeSet;

public class GMParser {

    public static GMContent parse(byte[] content) {

        ByteBuffer buffer = ByteBuffer.wrap(content);

        byte type = buffer.get();


        if (type == GMType.GARLIC_MESSAGE.getId()) {

            GarlicMessage garlicMessage = new GarlicMessage(content);
            garlicMessage.tryParseContent();

//            System.out.println("got new garlic message for me?: " + garlicMessage.isTargetedToUs());

            // if the gm is targeted to us the content will be handled by the parseContent routine of the gm
            if (!garlicMessage.isTargetedToUs()) {
                sendGarlicMessageToPeer(garlicMessage);
            }


            return garlicMessage;

        } else if (type == GMType.ACK.getId()) {


            GMAck gmAck = new GMAck(content);
            gmAck.parseContent();

//            System.out.println("got ack: " + gmAck.getAckid());

            Job runningJob = Job.getRunningJob(gmAck.getAckid());

            if (runningJob != null && runningJob instanceof PeerPerformanceTestFlaschenpostJob) {
                PeerPerformanceTestFlaschenpostJob perfJob = (PeerPerformanceTestFlaschenpostJob) runningJob;
//                System.out.println("GM Test finished in: " + perfJob.getEstimatedRuntime() + " ms");
                perfJob.success();
            }

            if (runningJob != null && runningJob instanceof PeerPerformanceTestGarlicMessageJob) {
                PeerPerformanceTestGarlicMessageJob perfJob = (PeerPerformanceTestGarlicMessageJob) runningJob;
//                System.out.println("GM Test finished in: " + perfJob.getEstimatedRuntime() + " ms");
                perfJob.success();
            }

            return gmAck;
        }

        throw new RuntimeException("Unknown GMType at parsing: " + type);
    }

    private static void sendGarlicMessageToPeer(GarlicMessage garlicMessage) {

//        boolean put = FPStoreManager.put(garlicMessage);
//
//        if (put) {
//            System.out.println("message already handled, do not send again, destination " + garlicMessage.getDestination() + " id " + garlicMessage.getId());
//            return;
//        } else {
//            System.out.println("handle fp with destination " + garlicMessage.getDestination() + " id " + garlicMessage.getId());
//        }


        Peer peerToSendFP = PeerList.get(garlicMessage.getDestination());

        byte[] content = garlicMessage.getContent();


        if (peerToSendFP == null || !peerToSendFP.isConnected()) {

            //todo, put all into a job to handle failing peers and retry send if no ack

            int myDistanceToKey = garlicMessage.getDestination().getDistance(Server.NONCE);

            TreeSet<Peer> peers = new TreeSet<>(new PeerComparator(garlicMessage.getDestination()));

            //todo use best route for this flaschenpost by network graph

            //insert all nodes
            PeerList.getReadWriteLock().readLock().lock();
            try {
                ArrayList<Peer> peerList = PeerList.getPeerArrayList();

                if (peerList == null) {
                    return;
                }

                for (Peer p : peerList) {

                    //do not add the peer if the peer is not connected or the nodeId is unknown!
                    if (p.getNodeId() == null || !p.isConnected()) {
                        continue;
                    }

                    //do not send fps to light clients
                    if (p.isLightClient()) {
                        continue;
                    }

                    /**
                     * do not add peers which are further or equally away from the key than us
                     */
                    int peersDistanceToKey = garlicMessage.getDestination().getDistance(p.getKademliaId());
                    if (myDistanceToKey <= peersDistanceToKey) {
                        continue;
                    }
//                    System.out.println("my distance: " + myDistanceToKey + " theirs distance: " + peersDistanceToKey);

                    peers.add(p);
                }
            } finally {
                PeerList.getReadWriteLock().readLock().unlock();
            }

            if (peers.size() == 0) {
                return;
            }

            peerToSendFP = peers.first();

//            int peersDistance = garlicMessage.getDestination().getDistance(peerToSendFP.getKademliaId());
//            System.out.println("inserting fp to peer " + garlicMessage.getDestination() + "  since we are not directly connected distance " + peersDistance + " our distance " + myDistanceToKey + " last " + garlicMessage.getDestination().getDistance(peers.last().getKademliaId()) + " node: " + peerToSendFP.getNode().getNodeId() + " con " + peerToSendFP.isConnected());

        }


        peerToSendFP.getWriteBufferLock().lock();
        try {
            peerToSendFP.writeBuffer.put(Command.FLASCHENPOST_PUT);
            peerToSendFP.writeBuffer.putInt(content.length);
            peerToSendFP.writeBuffer.put(content);
            peerToSendFP.setWriteBufferFilled();
//            System.out.println("send fp to other peer: " + garlicMessage.getDestination());
        } finally {
            peerToSendFP.getWriteBufferLock().unlock();
        }

    }

}
