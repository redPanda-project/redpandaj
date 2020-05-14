package im.redpanda.core;

import org.junit.runner.Result;

public class TestHelper {
    private static boolean running = false;


    public static void startInstance() {
        if (!running) {
            System.out.println("executing code before all tests are running");
            Server.start();
            System.out.println("redpanda instance starting...");
            running = true;
        }

        int cnt = 0;
        while (!Server.startedUpSuccessful) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("waiting for redpanda start...");
            cnt++;
            if (cnt > 100) {
                throw new RuntimeException("redpanda did not start within 10 seconds...");
            }
        }

    }


    public static void stopInstance() {
        System.out.println("all tests have finished, lets clean up...");
        Server.shutdown();
        System.out.println("redpanda shutdown complete...");
        running = false;
    }

}
