package im.redpanda.core;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

public class TestListener extends RunListener {

  private static boolean running = false;

  @Override
  public void testRunStarted(Description description) throws Exception {
    //        // Called before any tests have been run.
    //        if (!running) {
    //            System.out.println("executing code before all tests are running");
    //            Server.start();
    //            System.out.println("redpanda instance starting...");
    //            running = true;
    //        }
    //
    //        int cnt = 0;
    //        while (!Server.startedUpSuccessful) {
    //            Thread.sleep(50);
    //            System.out.println("waiting for redpanda start...");
    //            cnt++;
    //            if (cnt > 100) {
    //                throw new RuntimeException("redpanda did not start within 10 seconds...");
    //            }
    //        }

  }

  @Override
  public void testRunFinished(Result result) throws Exception {
    //        // Called when all tests have finished
    //        System.out.println("all tests have finished, lets clean up...");
    //        Server.shutdown();
    //        System.out.println("redpanda shutdown complete...");
  }
}
