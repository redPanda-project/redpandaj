package im.redpanda.jobs;

import im.redpanda.core.Command;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;

public class RequestPeerListJob extends Job {


    public RequestPeerListJob(ServerContext serverContext) {
        super(serverContext, 1000L * 60L * 1L, true);
    }

    @Override
    public void init() {
    }


    @Override
    public void work() {


        //todo request and send peers over garlic messages...

        try {
            Peer peer = serverContext.getPeerList().getGoodPeer(1.0f);
            peer.getWriteBufferLock().lock();
            try {
                peer.writeBuffer.put(Command.REQUEST_PEERLIST);
                peer.setWriteBufferFilled();
            } finally {
                peer.getWriteBufferLock().unlock();
            }
        } catch (Exception e) {

        }

    }


}
