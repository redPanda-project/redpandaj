package im.redpanda.jobs;

import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;

public class PeerCheckGarlicMesssageScheldulerJob extends Job {


    public PeerCheckGarlicMesssageScheldulerJob() {
        super(1000L * 10L, true);
    }

    @Override
    public void init() {
    }

    @Override
    public void work() {

        Peer p = PeerList.getGoodPeer();

//        p.get


    }
}
