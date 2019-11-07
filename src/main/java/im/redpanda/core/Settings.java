package im.redpanda.core;

public class Settings {

    public static int STD_PORT = 59558;
    public static int MIN_CONNECTIONS = 20;
    public static int MAX_CONNECTIONS = 50;
    public static int pingTimeout = 65; //time in sec
    public static int pingDelay = 30000; //time in msec
    public static int peerListRequestDelay = 60 * 60;//time in sec

    public static final int k = 20; //k value from kademlia (nodes in one bucket)

    public static boolean IPV6_ONLY = false;
    public static boolean IPV4_ONLY = false;

    public static String[] knownNodes = {"195.201.25.223:59558", "51.15.99.205:59558"};

}
