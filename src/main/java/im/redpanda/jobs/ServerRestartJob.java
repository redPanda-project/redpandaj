package im.redpanda.jobs;

import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import im.redpanda.core.Settings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class ServerRestartJob extends Job {

    private static final long STARTED_AT = System.currentTimeMillis();
    private long restartTime;

    public ServerRestartJob(ServerContext serverContext) {
        super(serverContext, 1000L * 60L * 60L, true);
        restartTime = 1000L * 60L * 60L * 12L * 1L + Server.secureRandom.nextInt(60 * 60 * 8) * 1000L;
    }

    @Override
    public void init() {
        //not needed
    }

    @Override
    public void work() {

        if (System.currentTimeMillis() - STARTED_AT < restartTime) {
            return;
        }

        if (!Settings.isLoadUpdates()) {
            return;
        }

        System.out.println("########################\n#    Server restart due to Job... #\n########################");

        File file = new File(".restart");
        if (!file.exists()) {
            try {
                new FileOutputStream(file).close();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("could not create .restart file, the restart will fail... exiting...");
            }
        }

        Server.shutdown(serverContext);
        System.exit(0);


    }
}
