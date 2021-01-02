package im.redpanda.jobs;

import im.redpanda.core.Server;

public class NodeStoreMaintainJob extends Job {


    public NodeStoreMaintainJob() {
        super(1000L * 30L * 1L, true);
    }

    @Override
    public void init() {
    }

    @Override
    public void work() {

        if (Server.nodeStore != null && !Server.SHUTDOWN) {
            Server.nodeStore.maintainNodes();
        }

    }
}
