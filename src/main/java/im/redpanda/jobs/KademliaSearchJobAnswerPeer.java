package im.redpanda.jobs;


import im.redpanda.core.Command;
import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.kademlia.KadContent;

import java.nio.ByteBuffer;

public class KademliaSearchJobAnswerPeer extends KademliaSearchJob {


    private Peer answerTo;
    private int ackID;

    public KademliaSearchJobAnswerPeer(KademliaId id, Peer answerTo, int ackID) {
        super(id);
        this.answerTo = answerTo;
        this.ackID = ackID;
    }


    @Override
    protected KadContent success() {

        KadContent kadContent = super.success();

        if (kadContent == null) {
            System.out.println("job failed, did not found an entry in time...");
            fail();
            return null;
        }

        System.out.println("we have to send the search answer to: " + answerTo.getKademliaId());

        if (!answerTo.isConnected()) {
            System.out.println("peer not online, do not answer the KadSearch: " + answerTo.getKademliaId());
            return kadContent;
        }

        answerTo.getWriteBufferLock().lock();
        try {
            answerTo.getWriteBuffer().put(Command.KADEMLIA_GET_ANSWER);
            answerTo.getWriteBuffer().putInt(ackID);
//            answerTo.getWriteBuffer().put(kadContent.getId().getBytes());
            answerTo.getWriteBuffer().putLong(kadContent.getTimestamp());
            answerTo.getWriteBuffer().put(kadContent.getPubkey());
            answerTo.getWriteBuffer().putInt(kadContent.getContent().length);
            answerTo.getWriteBuffer().put(kadContent.getContent());
            answerTo.getWriteBuffer().put(kadContent.getSignature());
        } finally {
            answerTo.getWriteBufferLock().unlock();
        }

        System.out.println("wrote search answer to: " + answerTo.getKademliaId());

        return kadContent;
    }


    @Override
    protected void fail() {
        super.fail();
        //todo: send fail to light client

    }
}
