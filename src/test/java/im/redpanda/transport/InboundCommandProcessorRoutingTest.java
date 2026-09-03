package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.Command;
import im.redpanda.dht.KademliaCommandHandler;
import im.redpanda.mailbox.OutboundCommandHandler;
import im.redpanda.routing.FlaschenpostCommandHandler;
import im.redpanda.updater.ApkUpdateHandler;
import im.redpanda.updater.JarUpdateHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins the wiring of the T116 dispatcher: every command byte must reach the handler method it is
 * supposed to reach, and the dispatcher must report the right number of consumed bytes.
 *
 * <p>The dispatch table test next door only checks that a command is registered <em>somehow</em>.
 * That would not notice a transposed wiring line — {@code handleFetch} registered under {@code
 * OUTBOUND_REVOKE_OH_REQ}, say — because both commands stay registered and both are framed. Nothing
 * else in the suite drives {@code parseCommand} for the five outbound request commands or for
 * {@code FLASCHENPOST_V2}: the outbound and garlic tests call their services directly. So a
 * transposition in the moved registrations would have compiled and gone green.
 *
 * <p>Every handler is replaced by a recorder that only writes down which method ran, so this test
 * asserts routing and byte accounting, not domain behaviour.
 */
class InboundCommandProcessorRoutingTest {

  private final List<String> calls = new ArrayList<>();

  private InboundCommandProcessor newProcessor() {
    return new InboundCommandProcessor(
        new RecordingPeerExchange(calls),
        new RecordingJarUpdate(calls),
        new RecordingApkUpdate(calls),
        new RecordingKademlia(calls),
        new RecordingFlaschenpost(calls),
        new RecordingOutbound(calls));
  }

  /** Builds the {@code [len:4][payload]} tail the dispatcher expects for a framed command. */
  private static ByteBuffer framed(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + payload.length);
    buffer.putInt(payload.length);
    buffer.put(payload);
    buffer.flip();
    return buffer;
  }

  private String routeFramed(byte command, byte[] payload) {
    calls.clear();
    int consumed = newProcessor().parseCommand(command, framed(payload), null);
    assertThat(consumed)
        .as("consumed bytes for framed command %s", command)
        .isEqualTo(1 + 4 + payload.length);
    assertThat(calls).hasSize(1);
    return calls.get(0);
  }

  private String routeBare(byte command) {
    calls.clear();
    int consumed = newProcessor().parseCommand(command, ByteBuffer.allocate(0), null);
    assertThat(consumed).as("consumed bytes for bare command %s", command).isEqualTo(1);
    assertThat(calls).hasSize(1);
    return calls.get(0);
  }

  @Test
  void peerListCommandsRouteToPeerExchange() {
    assertThat(routeBare(Command.PING)).isEqualTo("peer.ping");
    assertThat(routeBare(Command.PONG)).isEqualTo("peer.pong");
    assertThat(routeBare(Command.REQUEST_PEERLIST)).isEqualTo("peer.requestPeerList");
    assertThat(routeFramed(Command.SEND_PEERLIST, new byte[] {1, 2, 3}))
        .isEqualTo("peer.sendPeerList");
  }

  @Test
  void updateCommandsRouteToTheirUpdaterHandler() {
    assertThat(routeBare(Command.UPDATE_REQUEST_TIMESTAMP)).isEqualTo("jar.requestTimestamp");
    assertThat(routeBare(Command.UPDATE_ANSWER_TIMESTAMP)).isEqualTo("jar.answerTimestamp");
    assertThat(routeBare(Command.UPDATE_REQUEST_CONTENT)).isEqualTo("jar.requestContent");
    assertThat(routeBare(Command.UPDATE_ANSWER_CONTENT)).isEqualTo("jar.answerContent");
    assertThat(routeBare(Command.ANDROID_UPDATE_REQUEST_TIMESTAMP))
        .isEqualTo("apk.requestTimestamp");
    assertThat(routeBare(Command.ANDROID_UPDATE_ANSWER_TIMESTAMP)).isEqualTo("apk.answerTimestamp");
    assertThat(routeBare(Command.ANDROID_UPDATE_REQUEST_CONTENT)).isEqualTo("apk.requestContent");
    assertThat(routeBare(Command.ANDROID_UPDATE_ANSWER_CONTENT)).isEqualTo("apk.answerContent");
  }

  @Test
  void dhtCommandsRouteToTheKademliaHandler() {
    assertThat(routeFramed(Command.JOB_ACK, new byte[] {1})).isEqualTo("kad.jobAck");
    assertThat(routeFramed(Command.KADEMLIA_GET, new byte[] {1})).isEqualTo("kad.get");
    assertThat(routeFramed(Command.KADEMLIA_STORE, new byte[] {1})).isEqualTo("kad.store");
    assertThat(routeFramed(Command.KADEMLIA_GET_ANSWER, new byte[] {1})).isEqualTo("kad.getAnswer");
  }

  @Test
  void mailboxAndGarlicCommandsRouteToTheFlaschenpostHandler() {
    assertThat(routeFramed(Command.FLASCHENPOST_PUT, new byte[] {1})).isEqualTo("fp.put");
    assertThat(routeFramed(Command.FLASCHENPOST_V2, new byte[] {1})).isEqualTo("fp.v2");
  }

  /**
   * The five outbound request commands are the transposition-prone block: five identical shapes.
   */
  @Test
  void outboundRequestCommandsRouteToTheirOwnHandlerMethod() {
    assertThat(routeFramed(Command.OUTBOUND_REGISTER_OH_REQ, new byte[] {1}))
        .isEqualTo("outbound.register");
    assertThat(routeFramed(Command.OUTBOUND_FETCH_REQ, new byte[] {1})).isEqualTo("outbound.fetch");
    assertThat(routeFramed(Command.OUTBOUND_REVOKE_OH_REQ, new byte[] {1}))
        .isEqualTo("outbound.revoke");
    assertThat(routeFramed(Command.OUTBOUND_ACK_FETCH_REQ, new byte[] {1}))
        .isEqualTo("outbound.ackFetch");
    assertThat(routeFramed(Command.OUTBOUND_SUBSCRIBE_REQ, new byte[] {1}))
        .isEqualTo("outbound.subscribe");
  }

  /** The payload handed to a framed handler is the frame's payload, without the length prefix. */
  @Test
  void framedHandlersSeeTheExactPayloadBytes() {
    byte[] payload = new byte[] {9, 8, 7, 6, 5};
    List<byte[]> seen = new ArrayList<>();
    InboundCommandProcessor proc =
        new InboundCommandProcessor(
            new RecordingPeerExchange(calls),
            new RecordingJarUpdate(calls),
            new RecordingApkUpdate(calls),
            new RecordingKademlia(calls),
            new RecordingFlaschenpost(calls) {
              @Override
              public void handlePut(Peer peer, byte[] p) {
                seen.add(p);
              }
            },
            new RecordingOutbound(calls));

    proc.parseCommand(Command.FLASCHENPOST_PUT, framed(payload), null);

    assertThat(seen).hasSize(1);
    assertThat(seen.get(0)).containsExactly(payload);
  }

  /** An incomplete frame consumes nothing and must not reach any handler. */
  @Test
  void anIncompleteFrameIsNotDispatched() {
    ByteBuffer truncated = ByteBuffer.allocate(4 + 2);
    truncated.putInt(16); // claims 16 payload bytes
    truncated.put(new byte[] {1, 2});
    truncated.flip();

    assertThat(newProcessor().parseCommand(Command.FLASCHENPOST_PUT, truncated, null)).isZero();
    assertThat(calls).isEmpty();
    assertThat(truncated.position()).as("the buffer must be rewound for the next read").isZero();
  }

  // --- recorders -------------------------------------------------------------------------------

  private static class RecordingPeerExchange extends PeerExchangeHandler {
    private final List<String> calls;

    RecordingPeerExchange(List<String> calls) {
      super(null);
      this.calls = calls;
    }

    @Override
    int handlePing(Peer peer) {
      calls.add("peer.ping");
      return 1;
    }

    @Override
    int handlePong(Peer peer) {
      calls.add("peer.pong");
      return 1;
    }

    @Override
    int handleRequestPeerList(Peer peer) {
      calls.add("peer.requestPeerList");
      return 1;
    }

    @Override
    void handleSendPeerList(Peer peer, byte[] payload) {
      calls.add("peer.sendPeerList");
    }
  }

  private static class RecordingJarUpdate extends JarUpdateHandler {
    private final List<String> calls;

    RecordingJarUpdate(List<String> calls) {
      super(null);
      this.calls = calls;
    }

    @Override
    public int handleRequestTimestamp(Peer peer) {
      calls.add("jar.requestTimestamp");
      return 1;
    }

    @Override
    public int handleAnswerTimestamp(ByteBuffer readBuffer, Peer peer) {
      calls.add("jar.answerTimestamp");
      return 1;
    }

    @Override
    public int handleRequestContent(Peer peer) {
      calls.add("jar.requestContent");
      return 1;
    }

    @Override
    public int handleAnswerContent(ByteBuffer readBuffer, Peer peer) {
      calls.add("jar.answerContent");
      return 1;
    }
  }

  private static class RecordingApkUpdate extends ApkUpdateHandler {
    private final List<String> calls;

    RecordingApkUpdate(List<String> calls) {
      super(null);
      this.calls = calls;
    }

    @Override
    public int handleRequestTimestamp(Peer peer) {
      calls.add("apk.requestTimestamp");
      return 1;
    }

    @Override
    public int handleAnswerTimestamp(ByteBuffer readBuffer, Peer peer) {
      calls.add("apk.answerTimestamp");
      return 1;
    }

    @Override
    public int handleRequestContent(Peer peer) {
      calls.add("apk.requestContent");
      return 1;
    }

    @Override
    public int handleAnswerContent(ByteBuffer readBuffer, Peer peer) {
      calls.add("apk.answerContent");
      return 1;
    }
  }

  private static class RecordingKademlia extends KademliaCommandHandler {
    private final List<String> calls;

    RecordingKademlia(List<String> calls) {
      super(null);
      this.calls = calls;
    }

    @Override
    public void handleJobAck(Peer peer, byte[] payload) {
      calls.add("kad.jobAck");
    }

    @Override
    public void handleKademliaGet(Peer peer, byte[] payload) {
      calls.add("kad.get");
    }

    @Override
    public void handleKademliaStore(Peer peer, byte[] payload) {
      calls.add("kad.store");
    }

    @Override
    public void handleKademliaGetAnswer(Peer peer, byte[] payload) {
      calls.add("kad.getAnswer");
    }
  }

  private static class RecordingFlaschenpost extends FlaschenpostCommandHandler {
    private final List<String> calls;

    RecordingFlaschenpost(List<String> calls) {
      super(null, null);
      this.calls = calls;
    }

    @Override
    public void handlePut(Peer peer, byte[] payload) {
      calls.add("fp.put");
    }

    @Override
    public void handleV2(byte[] payload) {
      calls.add("fp.v2");
    }
  }

  private static class RecordingOutbound extends OutboundCommandHandler {
    private final List<String> calls;

    RecordingOutbound(List<String> calls) {
      super(null);
      this.calls = calls;
    }

    @Override
    public void handleRegister(Peer peer, byte[] payload) {
      calls.add("outbound.register");
    }

    @Override
    public void handleFetch(Peer peer, byte[] payload) {
      calls.add("outbound.fetch");
    }

    @Override
    public void handleRevoke(Peer peer, byte[] payload) {
      calls.add("outbound.revoke");
    }

    @Override
    public void handleAckFetch(Peer peer, byte[] payload) {
      calls.add("outbound.ackFetch");
    }

    @Override
    public void handleSubscribe(Peer peer, byte[] payload) {
      calls.add("outbound.subscribe");
    }
  }
}
