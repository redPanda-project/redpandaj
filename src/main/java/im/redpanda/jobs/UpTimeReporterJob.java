package im.redpanda.jobs;

import im.redpanda.core.ServerContext;

import java.time.Duration;

public class UpTimeReporterJob extends Job {


    public UpTimeReporterJob(ServerContext serverContext) {
        super(serverContext, Duration.ofMinutes(10).toMillis(), true);
    }

    @Override
    public void init() {
        //nothing to do
    }

    @Override
    public void work() {
        serverContext.getLocalSettings().getSystemUpTimeData().clearTooOldHits();
        serverContext.getLocalSettings().getSystemUpTimeData().reportNow();
    }
}
