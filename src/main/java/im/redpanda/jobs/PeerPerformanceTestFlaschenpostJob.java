package im.redpanda.jobs;

import im.redpanda.core.*;
import im.redpanda.flaschenpost.GMAck;
import im.redpanda.flaschenpost.GarlicMessage;

public class PeerPerformanceTestFlaschenpostJob extends Job {

    Peer peer;
    boolean success = false;

    public PeerPerformanceTestFlaschenpostJob(Peer peer) {
        this.peer = peer;
    }

    @Override
    public void init() {

        cleanPeerChecks();

        System.out.println("we are creating a Flaschenpost to monitor other peers...");


        //lets target to ourselves without the private key!
        NodeId targetId = NodeId.importPublic(Server.nodeId.exportPublic());

        GMAck gmAck = new GMAck(getJobId());

        GarlicMessage garlicMessage = new GarlicMessage(targetId);
        garlicMessage.addGMContent(gmAck);

        byte[] content = garlicMessage.getContent();

        if (!peer.isConnected() || peer.getNode() == null) {
            return;
        }

        peer.getWriteBufferLock().lock();
        try {
            peer.getWriteBuffer().put(Command.FLASCHENPOST_PUT);
            peer.getWriteBuffer().putInt(content.length);
            peer.getWriteBuffer().put(content);
            peer.setWriteBufferFilled();
        } finally {
            peer.getWriteBufferLock().unlock();
        }


    }

    private void cleanPeerChecks() {
        Node node = peer.getNode();
        if (node != null) {
            if (node.getGmTestsFailed() > 200) {
                node.setGmTestsFailed(200);
            }

            if (node.getGmTestsSuccessful() > 200) {
                node.setGmTestsSuccessful(200);
                if (node.getGmTestsFailed() > 0) {
                    node.setGmTestsFailed(node.getGmTestsFailed() - 1);
                    node.setGmTestsSuccessful(100);
                }
            }

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
