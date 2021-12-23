package im.redpanda.core;

import java.io.File;

public class Settings {

    public static final int DEFAULT_PORT = 59558;
    public static boolean DEBUG = true;
    public static boolean NAT_OPEN = false;
    public static int STD_PORT = 59558;
    public static int MIN_CONNECTIONS = 20;
    public static int MAX_CONNECTIONS = 50;
    public static long pingTimeout = 65L * 1000L; //time in ms
    public static int pingDelay = 1000; //time in ms
    public static int peerListRequestDelay = 60 * 60;//time in sec
    public static boolean seedNode;
    public static boolean loadUpdates;

    public static final String SAVE_DIR = "data";

    public static final int k = 20; //k value from kademlia (nodes in one bucket)

    public static boolean IPV6_ONLY = false;
    public static boolean IPV4_ONLY = false;

    public static void init(ServerContext serverContext) {
        File file = new File("redpanda.jar");
        if (!file.exists()) {
            System.out.println("No jar to update found, disable auto update");
            loadUpdates = false;

            file = new File("target/redpanda.jar");
            if (file.exists() && serverContext.getPort() == DEFAULT_PORT) {
                System.out.println("found compiled jar, this is a seed node");
                seedNode = true;
            }
        } else {
            loadUpdates = true;
        }

    }

    public static String[] knownNodes = {"195.201.25.223:59558", "redpanda.im:59559"};

    public static String[] blacklistIps = {"127.0.0.1"};

    public static int getStartPort() {
        return STD_PORT;
    }

    public static boolean isSeedNode() {
        return seedNode;
    }

    public static boolean isLoadUpdates() {
        return loadUpdates;
    }

}
