package im.redpanda.flaschenpost;

import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import im.redpanda.core.Server;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class GarlicMessage extends Flaschenpost {

    /**
     * This is the {@link NodeId} containing the public key of the target {@link im.redpanda.core.Peer}/{@link im.redpanda.core.Node}
     * and is only used for the creation process.
     */
    private NodeId targetsNodeId;
    /**
     * The public key of the target.
     */
    private byte[] publicKey;
    /**
     * ACK_ID which should be encrypted as well.
     */
    private int ack_id;
    /**
     * The Content of the GarlicMessage is a List of other GarlicMessages.
     */
    private byte[] iv;
    private ArrayList<GMContent> nestedMessages;

    public GarlicMessage(NodeId targetsNodeId) {
        this.targetsNodeId = targetsNodeId;
        this.publicKey = targetsNodeId.exportPublic();
        this.ack_id = Server.random.nextInt();
        this.nestedMessages = new ArrayList<>();
        this.iv = new byte[16];
        Server.secureRandom.nextBytes(this.iv);
    }

    public void addGMContent(GMContent gmContent) {
        nestedMessages.add(gmContent);
    }

    @Override
    protected void computeContent() {

        int bytesForContent = 0;
        for (GMContent c : nestedMessages) {
            bytesForContent += c.getContent().length;
        }

        ByteBuffer b = ByteBuffer.allocate(KademliaId.ID_LENGTH_BYTES + iv.length + NodeId.PUBLIC_KEYLEN + 4 + bytesForContent + 72);
        b.put(targetsNodeId.getKademliaId().getBytes());
        b.put(iv);
        b.put(publicKey);
        b.putInt(nestedMessages.size());
        for (GMContent c : nestedMessages) {
           b.put(c.getContent());
        }



    }

    @Override
    public GMType getGMType() {
        return GMType.GARLIC_MESSAGE;
    }
}
