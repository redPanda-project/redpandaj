package im.redpanda.jobs;

import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;

public class NodeStoreMaintainJob extends Job {


    public NodeStoreMaintainJob(ServerContext serverContext) {
        super(serverContext, 1000L * 5L * 1L, true);
    }

    @Override
    public void init() {
        // no need for job setup
    }

    @Override
    public void work() {

        if (serverContext.getNodeStore() != null && !Server.SHUTDOWN) {
            serverContext.getNodeStore().maintainNodes();
        }

    }
}
