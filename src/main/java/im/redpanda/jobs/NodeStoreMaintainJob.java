package im.redpanda.jobs;

import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;

public class NodeStoreMaintainJob extends Job {


    public NodeStoreMaintainJob(ServerContext serverContext) {
        super(serverContext, 1000L * 30L * 1L, true);
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
