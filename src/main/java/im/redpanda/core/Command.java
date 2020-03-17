package im.redpanda.core;

public class Command {

    public static final byte REQUEST_PUBLIC_KEY = (byte) 1;
    public static final byte SEND_PUBLIC_KEY = (byte) 2;

    public static final byte ACTIVATE_ENCRYPTION = (byte) 3;
    public static final byte PING = (byte) 5;
    public static final byte PONG = (byte) 6;

    public static final byte REQUEST_PEERLIST = (byte) 7;
    public static final byte SEND_PEERLIST = (byte) 8;
    public static final byte UPDATE_REQUEST_TIMESTAMP = (byte) 9;
    public static final byte UPDATE_ANSWER_TIMESTAMP = (byte) 10;
    public static final byte UPDATE_REQUEST_CONTENT = (byte) 11;
    public static final byte UPDATE_ANSWER_CONTENT = (byte) 12;

    //kademlia cmds
    public static final byte KADEMLIA_STORE = (byte) 120;// standalone command
    public static final byte KADEMLIA_GET = (byte) 121;// standalone command
    public static final byte KADEMLIA_GET_ANSWER = (byte) 122;// standalone command
    public static final byte JOB_ACK = (byte) 130;// standalone command

}
