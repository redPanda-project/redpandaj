package im.redpanda.jobs;

import im.redpanda.core.ServerContext;

public class SaveJobs extends Job {


    public SaveJobs(ServerContext serverContext) {
        super(serverContext, 1000L * 60L * 5L, true);
    }

    @Override
    public void init() {
        // no need for job setup
    }


    @Override
    public void work() {
        serverContext.getLocalSettings().save(serverContext.getPort());
    }


}
