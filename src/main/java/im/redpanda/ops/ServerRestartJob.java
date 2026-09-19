package im.redpanda.ops;

import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
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
    // not needed
  }

  @Override
  public void work() {

    if (System.currentTimeMillis() - STARTED_AT < restartTime) {
      return;
    }

    if (!Settings.isLoadUpdates()) {
      return;
    }

    System.out.println(
        "########################\n#    Server restart due to Job... #\n########################");

    File file = new File(".restart");
    if (!file.exists()) {
      try {
        new FileOutputStream(file).close();
      } catch (IOException e) {
        e.printStackTrace();
        System.out.println("could not create .restart file, the restart will fail... exiting...");
      }
    }

    // System.exit in a finally: the restart is the point of this job, and since TD225 a throw out
    // of work() no longer ends a permanent job -- without the finally, a failing shutdown (full
    // disk, an unreadable store) would leave the process running with shuttingDown already set,
    // i.e. a node that does no I/O and only restarts an hour later. Exiting anyway also gives the
    // JVM hook its retry at the save, since Server.shutdown() only marks itself done on success.
    try {
      Server.shutdown(serverContext);
    } finally {
      System.exit(0);
    }
  }
}
