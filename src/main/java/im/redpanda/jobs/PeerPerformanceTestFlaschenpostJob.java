package im.redpanda.jobs;

import im.redpanda.core.Command;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.core.Server;
import im.redpanda.flaschenpost.GMAck;
import im.redpanda.flaschenpost.GMContent;
import im.redpanda.flaschenpost.GMParser;
import im.redpanda.flaschenpost.GarlicMessage;

import java.nio.ByteBuffer;

public class PeerPerformanceTestFlaschenpostJob extends Job {

    Peer peer;

    public PeerPerformanceTestFlaschenpostJob(Peer peer) {
        this.peer = peer;
    }

    @Override
    public void init() {

        System.out.println("we are creating a falschenpost to monitor other peers...");


        //lets target to ourselves without the private key!
        NodeId targetId = NodeId.importPublic(Server.nodeId.exportPublic());

        GMAck gmAck = new GMAck(getJobId());

        GarlicMessage garlicMessage = new GarlicMessage(targetId);
        garlicMessage.addGMContent(gmAck);

        byte[] content = garlicMessage.getContent();

        GMContent parse = GMParser.parse(ByteBuffer.wrap(content));

        peer.getWriteBufferLock().lock();
        try {
//            peer.getWriteBuffer().put(Command.FLASCHENPOST_PUT);
//            peer.getWriteBuffer().putInt(garlicMessage.);
////            peer.getWriteBuffer().put(kadContent.getId().getBytes());
//            peer.getWriteBuffer().putLong(kadContent.getTimestamp());
//            peer.getWriteBuffer().put(kadContent.getPubkey());
//            peer.getWriteBuffer().putInt(kadContent.getContent().length);
//            peer.getWriteBuffer().put(kadContent.getContent());
//            peer.getWriteBuffer().put(kadContent.getSignature());
        } finally {
            peer.getWriteBufferLock().unlock();
        }


    }

    @Override
    public void work() {


    }
}
