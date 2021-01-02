package im.redpanda.jobs;

import im.redpanda.core.Command;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;

public class RequestPeerListJob extends Job {


    public RequestPeerListJob() {
        super(1000L * 30L * 1L, true);
    }

    @Override
    public void init() {
    }


    @Override
    public void work() {

        Peer peer = PeerList.getGoodPeer(1.0f);

        try {
            peer.getWriteBufferLock().lock();
            try {
                peer.writeBuffer.put(Command.REQUEST_PEERLIST);
                peer.setWriteBufferFilled();
                System.out.println("requested peerlist from peer");
            } finally {
                peer.getWriteBufferLock().unlock();
            }
        } catch (Exception e) {

        }

    }


}
