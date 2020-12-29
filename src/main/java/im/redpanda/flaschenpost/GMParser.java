package im.redpanda.flaschenpost;

import im.redpanda.core.Command;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.jobs.Job;
import im.redpanda.jobs.PeerPerformanceTestFlaschenpostJob;
import im.redpanda.jobs.PeerPerformanceTestGarlicMessageJob;

import java.nio.ByteBuffer;

public class GMParser {

    public static GMContent parse(byte[] content) {

        ByteBuffer buffer = ByteBuffer.wrap(content);

        byte type = buffer.get();


        if (type == GMType.GARLIC_MESSAGE.getId()) {

            GarlicMessage garlicMessage = new GarlicMessage(content);
            garlicMessage.tryParseContent();

            System.out.println("got new garlic message for me?: " + garlicMessage.isTargetedToUs());

            if (!garlicMessage.isTargetedToUs()) {
                sendGarlicMessageToPeer(garlicMessage);
            } else {
//                for (GMContent innerContent : garlicMessage.getGMContent()) {
//
//                    if (innerContent instanceof GMAck) {
//                        GMAck gmAck = (GMAck) innerContent;
//
//                        Job runningJob = Job.getRunningJob(gmAck.getAckid());
//
//                        if (runningJob != null && runningJob instanceof PeerPerformanceTestFlaschenpostJob) {
//                            PeerPerformanceTestFlaschenpostJob perfJob = (PeerPerformanceTestFlaschenpostJob) runningJob;
//                            System.out.println("GM Test finished in: " + perfJob.getEstimatedRuntime() + " ms");
//                            perfJob.success();
//                        }
//                    } else if (innerContent instanceof GarlicMessage) {
//
//                        GarlicMessage innerGarlicMessage = (GarlicMessage) innerContent;
//
//                        if (innerGarlicMessage.isTargetedToUs()) {
//                            GMParser.parse(innerGarlicMessage.getContent());
//                        } else {
//                            sendGarlicMessageToPeer(innerGarlicMessage);
//                        }
//
//                    }
//
//                }

            }


            return garlicMessage;

        } else if (type == GMType.ACK.getId()) {


            GMAck gmAck = new GMAck(content);
            gmAck.parseContent();

            System.out.println("got ack: " + gmAck.getAckid());

            Job runningJob = Job.getRunningJob(gmAck.getAckid());

            if (runningJob != null && runningJob instanceof PeerPerformanceTestFlaschenpostJob) {
                PeerPerformanceTestFlaschenpostJob perfJob = (PeerPerformanceTestFlaschenpostJob) runningJob;
                System.out.println("GM Test finished in: " + perfJob.getEstimatedRuntime() + " ms");
                perfJob.success();
            }

            if (runningJob != null && runningJob instanceof PeerPerformanceTestGarlicMessageJob) {
                PeerPerformanceTestGarlicMessageJob perfJob = (PeerPerformanceTestGarlicMessageJob) runningJob;
                System.out.println("GM Test finished in: " + perfJob.getEstimatedRuntime() + " ms");
                perfJob.success();
            }

            return gmAck;
        }

        throw new RuntimeException("Unknown GMType at parsing: " + type);
    }

    private static void sendGarlicMessageToPeer(GarlicMessage garlicMessage) {


        Peer peerToSendFP = PeerList.get(garlicMessage.getDestination());

        byte[] content = garlicMessage.getContent();


        if (peerToSendFP != null && peerToSendFP.isConnected()) {

            peerToSendFP.getWriteBufferLock().lock();
            try {
                peerToSendFP.writeBuffer.put(Command.FLASCHENPOST_PUT);
                peerToSendFP.writeBuffer.putInt(content.length);
                peerToSendFP.writeBuffer.put(content);
                peerToSendFP.setWriteBufferFilled();
                System.out.println("send gm to other peer: " + garlicMessage.getDestination());
            } finally {
                peerToSendFP.getWriteBufferLock().unlock();
            }
        } else {
            //todo
            System.out.println("todo: could not send gm to peer since we are not directly connected");
        }
    }

}
