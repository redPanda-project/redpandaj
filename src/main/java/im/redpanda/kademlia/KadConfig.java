package im.redpanda.kademlia;


import java.io.File;

public class KadConfig {

    private final static long RESTORE_INTERVAL = 5 * 60 * 1000; // in milliseconds
    private final static long RESPONSE_TIMEOUT = 2000;
    public static long OPERATION_TIMEOUT = 2000;
    private final static int CONCURRENCY = 10;
    private final static int K = 3;
    private final static int RCSIZE = 3;
    private final static int STALE = 10;
    private final static String LOCAL_FOLDER = "kademlia";

    private final static boolean IS_TESTING = false;


}
