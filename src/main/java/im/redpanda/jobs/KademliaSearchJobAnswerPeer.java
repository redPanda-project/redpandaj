package im.redpanda.jobs;

import im.redpanda.core.Command;
import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.kademlia.KadContent;

import java.util.ArrayList;

public class KademliaSearchJobAnswerPeer extends KademliaSearchJob {

    private final Peer answerTo;
    private final int ackID;

    public KademliaSearchJobAnswerPeer(ServerContext serverContext, KademliaId id, Peer answerTo, int ackID) {
        super(serverContext, id);
        this.answerTo = answerTo;
        this.ackID = ackID;
    }

    @Override
    protected ArrayList<KadContent> success() {

        ArrayList<KadContent> kadContents = super.success();

        if (kadContents == null || kadContents.get(0) == null) {
            System.out.println("job failed, did not found an entry in time...");
            fail();
            return null;
        }

        if (!answerTo.isConnected()) {
            return kadContents;
        }

        /**
         * write the least 3 newst entries...
         */
        for (int i = 0; i < Math.min(3, kadContents.size()); i++) {

            KadContent kadContent = kadContents.get(i);

            answerTo.getWriteBufferLock().lock();
            try {
                im.redpanda.proto.KademliaGetAnswer answerMsg = im.redpanda.proto.KademliaGetAnswer.newBuilder()
                        .setAckId(ackID)
                        .setTimestamp(kadContent.getTimestamp())
                        .setPublicKey(com.google.protobuf.ByteString.copyFrom(kadContent.getPubkey()))
                        .setContent(com.google.protobuf.ByteString.copyFrom(kadContent.getContent()))
                        .setSignature(com.google.protobuf.ByteString.copyFrom(kadContent.getSignature()))
                        .build();
                byte[] data = answerMsg.toByteArray();

                answerTo.getWriteBuffer().put(Command.KADEMLIA_GET_ANSWER);
                answerTo.getWriteBuffer().putInt(data.length);
                answerTo.getWriteBuffer().put(data);
            } finally {
                answerTo.getWriteBufferLock().unlock();
            }
        }

        return kadContents;
    }

    @Override
    protected void fail() {
        super.fail();
        // todo: send fail to light client

    }
}
