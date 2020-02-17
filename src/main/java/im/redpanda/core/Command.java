package im.redpanda.core;

public class Command {

    public static final byte REQUEST_PUBLIC_KEY = (byte) 1;
    public static final byte SEND_PUBLIC_KEY = (byte) 2;

    public static final byte ACTIVATE_ENCRYPTION = (byte) 3;
    public static final byte PING = (byte) 5;

    public static final byte REQUEST_PEERLIST = (byte) 6;
    public static final byte SEND_PEERLIST = (byte) 7;
}
