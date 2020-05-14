package im.redpanda.jobs;

import im.redpanda.core.Peer;

public class PeerCheckGarlicMesssageJob extends Job {

    Peer peer;

    public PeerCheckGarlicMesssageJob(Peer peer) {
        this.peer = peer;
    }

    @Override
    public void init() {

    }

    @Override
    public void work() {

    }
}
