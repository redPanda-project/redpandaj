package im.redpanda.core;

/**
 * Top-level wire command bytes: the first byte of every frame on a peer connection.
 *
 * <p>This class holds <b>nothing but command bytes</b>, each carrying the {@link WireCommand}
 * marker: {@link WireRegistry} renders exactly the marked constants into the wire registry, and
 * {@code WireRegistryTest} fails on any {@code public static final byte} here without the marker. A
 * constant that is not a command does not belong in this class.
 */
public final class Command {

  private Command() {}

  @WireCommand public static final byte REQUEST_PUBLIC_KEY = (byte) 1;
  @WireCommand public static final byte SEND_PUBLIC_KEY = (byte) 2;

  @WireCommand public static final byte ACTIVATE_ENCRYPTION = (byte) 3;
  @WireCommand public static final byte PING = (byte) 5;
  @WireCommand public static final byte PONG = (byte) 6;

  @WireCommand public static final byte REQUEST_PEERLIST = (byte) 7;
  @WireCommand public static final byte SEND_PEERLIST = (byte) 8;
  @WireCommand public static final byte UPDATE_REQUEST_TIMESTAMP = (byte) 9;
  @WireCommand public static final byte UPDATE_ANSWER_TIMESTAMP = (byte) 10;
  @WireCommand public static final byte UPDATE_REQUEST_CONTENT = (byte) 11;
  @WireCommand public static final byte UPDATE_ANSWER_CONTENT = (byte) 12;

  @WireCommand
  public static final byte ANDROID_UPDATE_REQUEST_TIMESTAMP = (byte) 13; // standalone command

  @WireCommand
  public static final byte ANDROID_UPDATE_ANSWER_TIMESTAMP = (byte) 14; // update timestamp: 1 long

  @WireCommand public static final byte ANDROID_UPDATE_REQUEST_CONTENT = (byte) 15; //
  @WireCommand public static final byte ANDROID_UPDATE_ANSWER_CONTENT = (byte) 16; //

  // kademlia cmds
  @WireCommand public static final byte KADEMLIA_STORE = (byte) 120;
  @WireCommand public static final byte KADEMLIA_GET = (byte) 121;
  @WireCommand public static final byte KADEMLIA_GET_ANSWER = (byte) 122;
  @WireCommand public static final byte JOB_ACK = (byte) 130;

  // flaschenpost
  @WireCommand public static final byte FLASCHENPOST_PUT = (byte) 141;

  /**
   * MS04: fixed-size (2048 byte) multi-hop garlic packet, see {@link
   * im.redpanda.routing.FlaschenpostV2}. Framed like every payload command: {@code
   * [cmd][len:4][packet]}.
   */
  @WireCommand public static final byte FLASCHENPOST_V2 = (byte) 142;

  // outbound
  @WireCommand public static final byte OUTBOUND_REGISTER_OH_REQ = (byte) 150;
  @WireCommand public static final byte OUTBOUND_REGISTER_OH_RES = (byte) 151;
  @WireCommand public static final byte OUTBOUND_FETCH_REQ = (byte) 152;
  @WireCommand public static final byte OUTBOUND_FETCH_RES = (byte) 153;
  @WireCommand public static final byte OUTBOUND_REVOKE_OH_REQ = (byte) 154;
  @WireCommand public static final byte OUTBOUND_REVOKE_OH_RES = (byte) 155;
  @WireCommand public static final byte OUTBOUND_ACK_FETCH_REQ = (byte) 156;
  @WireCommand public static final byte OUTBOUND_ACK_FETCH_RES = (byte) 157;
  @WireCommand public static final byte FLASCHENPOST_PUT_RES = (byte) 158;

  // Connection-Notify (T38): opt-in "new mail" signal over the existing peer connection.
  // Subscribe is a signed request/response (ownership proof reuses the Fetch signing scheme);
  // Notify is a one-way node → client message carrying only the oh_id. Never sent to clients
  // that did not subscribe (unknown commands desync their read loop).
  @WireCommand public static final byte OUTBOUND_SUBSCRIBE_REQ = (byte) 159;
  @WireCommand public static final byte OUTBOUND_SUBSCRIBE_RES = (byte) 160;
  @WireCommand public static final byte OUTBOUND_NOTIFY = (byte) 161;
}
