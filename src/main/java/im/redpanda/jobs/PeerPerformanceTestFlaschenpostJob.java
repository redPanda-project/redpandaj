package im.redpanda.jobs;

import im.redpanda.core.Command;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.flaschenpost.GMAck;
import im.redpanda.flaschenpost.GarlicMessage;

public class PeerPerformanceTestFlaschenpostJob extends Job {

    Peer peer;
    boolean success = false;

    public PeerPerformanceTestFlaschenpostJob(ServerContext serverContext, Peer peer) {
        super(serverContext);
        this.peer = peer;
    }

    @Override
    public void init() {

        peer.getNode().cleanChecks();

        System.out.println("we are creating a Flaschenpost to monitor other peers...");

        // lets target to ourselves without the private key!
        NodeId targetId = NodeId.importPublic(serverContext.getNodeId().exportPublic());

        GMAck gmAck = new GMAck(getJobId());

        GarlicMessage garlicMessage = new GarlicMessage(serverContext, targetId);
        garlicMessage.addGMContent(gmAck);

        byte[] content = garlicMessage.getContent();

        if (!peer.isConnected() || peer.getNode() == null) {
            return;
        }

        peer.getWriteBufferLock().lock();
        try {
            im.redpanda.proto.FlaschenpostPut putMsg = im.redpanda.proto.FlaschenpostPut.newBuilder()
                    .setContent(com.google.protobuf.ByteString.copyFrom(content))
                    .build();
            byte[] data = putMsg.toByteArray();

            peer.getWriteBuffer().put(Command.FLASCHENPOST_PUT);
            peer.getWriteBuffer().putInt(data.length);
            peer.getWriteBuffer().put(data);
            peer.setWriteBufferFilled();
        } finally {
            peer.getWriteBufferLock().unlock();
        }

    }

    @Override
    public void work() {
    }

    @Override
    public void done() {
        super.done();
        if (success) {
            peer.getNode().increaseGmTestsSuccessful();
        } else {
            peer.getNode().increaseGmTestsFailed();
        }

    }

    public void success() {
        success = true;
        done();
    }
}
