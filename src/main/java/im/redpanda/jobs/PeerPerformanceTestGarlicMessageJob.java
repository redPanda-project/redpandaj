package im.redpanda.jobs;

import im.redpanda.core.*;
import im.redpanda.flaschenpost.GMAck;
import im.redpanda.flaschenpost.GarlicMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class PeerPerformanceTestGarlicMessageJob extends Job {

    ArrayList<Node> nodes;
    boolean success = false;
    private Peer flaschenPostInsertPeer;

    public PeerPerformanceTestGarlicMessageJob() {
        nodes = new ArrayList<>();
    }

    public static byte[] calculateNestedGarlicMessages(ArrayList<Node> nodes, int jobId) {
        //lets target to ourselves without the private key!
        NodeId targetId = NodeId.importPublic(Server.nodeId.exportPublic());

        GMAck gmAck = new GMAck(jobId);

        GarlicMessage currentLayer = new GarlicMessage(targetId);
        currentLayer.addGMContent(gmAck);


        for (Node node : nodes) {

            GarlicMessage newLayer = new GarlicMessage(node.getNodeId());
            newLayer.addGMContent(currentLayer);

            currentLayer = newLayer;

        }


        byte[] content = currentLayer.getContent();
        return content;
    }

    @Override
    public void init() {

        flaschenPostInsertPeer = PeerList.getGoodPeer();

        if (!flaschenPostInsertPeer.isConnected() || flaschenPostInsertPeer.getNode() == null) {
            return;
        }

        flaschenPostInsertPeer.getNode().cleanChecks();

        int garlicSequenceLenght = 2;

        HashMap<KademliaId, Node> fastNodes = Server.nodeStore.getFastNodes();
        if (fastNodes.size() < garlicSequenceLenght) {
            System.out.println("not enough nodes");
            return;
        }

        ArrayList<Node> values = new ArrayList<>(fastNodes.values());

        Collections.shuffle(values);

        for (Node node : values) {
            if (nodes.size() == garlicSequenceLenght) {
                break;
            }
            node.cleanChecks();
            nodes.add(node);
        }


        byte[] content = calculateNestedGarlicMessages(nodes, getJobId());

        System.out.println("gm bytes computed with " + nodes.size() + " nodes.");

        flaschenPostInsertPeer.getWriteBufferLock().lock();
        try {
            flaschenPostInsertPeer.getWriteBuffer().put(Command.FLASCHENPOST_PUT);
            flaschenPostInsertPeer.getWriteBuffer().putInt(content.length);
            flaschenPostInsertPeer.getWriteBuffer().put(content);
            flaschenPostInsertPeer.setWriteBufferFilled();
        } finally {
            flaschenPostInsertPeer.getWriteBufferLock().unlock();
        }


    }

    @Override
    public void work() {
    }

    @Override
    public void done() {
        super.done();
        if (success) {
            flaschenPostInsertPeer.getNode().increaseGmTestsSuccessful();

            for (Node node : nodes) {
                node.increaseGmTestsSuccessful();
            }


        } else {
            flaschenPostInsertPeer.getNode().increaseGmTestsFailed();
            for (Node node : nodes) {
                node.increaseGmTestsFailed();
            }
        }

    }

    public void success() {
        success = true;
        done();
    }
}
