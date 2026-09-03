package im.redpanda.core;

import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.dht.KademliaCommandHandler;
import im.redpanda.mailbox.OutboundCommandHandler;
import im.redpanda.ops.Log;
import im.redpanda.routing.FlaschenpostCommandHandler;
import im.redpanda.updater.ApkUpdateHandler;
import im.redpanda.updater.JarUpdateHandler;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Dispatches inbound commands of a peer connection.
 *
 * <p>After T116 (DDD review 2026-08-31, §6 P2 step 2) this class owns the <em>framing</em> and
 * nothing else: read the command byte, pre-read the {@code [len:4][payload]} prefix for the framed
 * commands, route to a domain handler, account the consumed bytes, and disconnect on a protocol
 * desync. What a command <em>means</em> belongs to its bounded context:
 *
 * <table>
 *   <caption>command → domain</caption>
 *   <tr><td>5–8</td><td>{@link PeerExchangeHandler} (peer list / liveness, this package)</td></tr>
 *   <tr><td>9–16</td><td>{@link JarUpdateHandler} / {@link ApkUpdateHandler} (N-UPDATER)</td></tr>
 *   <tr><td>120–122, 130</td><td>{@link KademliaCommandHandler} (DHT)</td></tr>
 *   <tr><td>141, 142</td><td>{@link FlaschenpostCommandHandler} (mailbox / garlic routing)</td></tr>
 *   <tr><td>150–159 (requests)</td><td>{@link OutboundCommandHandler} (mailbox)</td></tr>
 * </table>
 *
 * <p>Commands 1–3 (public-key exchange, encryption activation) never reach this class — they are
 * consumed by the handshake in {@code ConnectionReaderThread}.
 */
public class InboundCommandProcessor {
  private static final Logger logger = LogManager.getLogger();

  @FunctionalInterface
  private interface CommandHandler {
    int handle(Peer peer, ByteBuffer readBuffer, byte[] payload)
        throws InvalidProtocolBufferException;
  }

  private final Map<Byte, CommandHandler> commandHandlers = new HashMap<>();

  /**
   * The commands whose payload is length-prefixed on the wire. Derived from the registrations below
   * instead of a second hand-maintained list, so a command can no longer be registered with one
   * framing and parsed with the other.
   */
  private final Set<Byte> framedCommands = new HashSet<>();

  private final PeerExchangeHandler peerExchangeHandler;
  private final JarUpdateHandler jarUpdateHandler;
  private final ApkUpdateHandler apkUpdateHandler;
  private final KademliaCommandHandler kademliaHandler;
  private final FlaschenpostCommandHandler flaschenpostHandler;
  private final OutboundCommandHandler outboundHandler;

  public InboundCommandProcessor(ServerContext serverContext) {
    this(
        new PeerExchangeHandler(serverContext),
        new JarUpdateHandler(serverContext),
        new ApkUpdateHandler(serverContext),
        new KademliaCommandHandler(serverContext),
        new FlaschenpostCommandHandler(serverContext, serverContext.getOutboundService()),
        new OutboundCommandHandler(serverContext.getOutboundService()));
  }

  /**
   * Seam for {@code InboundCommandProcessorRoutingTest}: lets a test hand in recording handlers and
   * assert that every command byte reaches the handler method it is supposed to reach. Without it,
   * a transposed wiring line (say {@code handleFetch} registered under {@code
   * OUTBOUND_REVOKE_OH_REQ}) compiles and passes every other test.
   */
  InboundCommandProcessor(
      PeerExchangeHandler peerExchangeHandler,
      JarUpdateHandler jarUpdateHandler,
      ApkUpdateHandler apkUpdateHandler,
      KademliaCommandHandler kademliaHandler,
      FlaschenpostCommandHandler flaschenpostHandler,
      OutboundCommandHandler outboundHandler) {
    this.peerExchangeHandler = peerExchangeHandler;
    this.jarUpdateHandler = jarUpdateHandler;
    this.apkUpdateHandler = apkUpdateHandler;
    this.kademliaHandler = kademliaHandler;
    this.flaschenpostHandler = flaschenpostHandler;
    this.outboundHandler = outboundHandler;
    initializeHandlers();
  }

  /** Registers a bare command: one byte on the wire, no payload. */
  private void bare(byte command, CommandHandler handler) {
    commandHandlers.put(command, handler);
  }

  /**
   * Registers a length-prefixed command: {@code [cmd][len:4][payload]}. The payload is read by
   * {@link #parseCommand(byte, ByteBuffer, Peer)} before the handler runs, and the handler's
   * declared consumption is the payload only — the {@code 1 + 4} framing bytes are added here,
   * once, instead of in every registration.
   */
  private void framed(byte command, FramedHandler handler) {
    framedCommands.add(command);
    commandHandlers.put(
        command,
        (peer, buf, payload) -> {
          handler.handle(peer, payload);
          return 1 + 4 + payload.length;
        });
  }

  @FunctionalInterface
  private interface FramedHandler {
    void handle(Peer peer, byte[] payload) throws InvalidProtocolBufferException;
  }

  private void initializeHandlers() {
    // Peer list / liveness (this package)
    bare(Command.PING, (peer, buf, payload) -> peerExchangeHandler.handlePing(peer));
    bare(Command.PONG, (peer, buf, payload) -> peerExchangeHandler.handlePong(peer));
    bare(
        Command.REQUEST_PEERLIST,
        (peer, buf, payload) -> peerExchangeHandler.handleRequestPeerList(peer));
    framed(Command.SEND_PEERLIST, peerExchangeHandler::handleSendPeerList);

    // N-UPDATER: software distribution is its own bounded context (im.redpanda.updater).
    bare(
        Command.UPDATE_REQUEST_TIMESTAMP,
        (peer, buf, payload) -> jarUpdateHandler.handleRequestTimestamp(peer));
    bare(
        Command.UPDATE_ANSWER_TIMESTAMP,
        (peer, buf, payload) -> jarUpdateHandler.handleAnswerTimestamp(buf, peer));
    bare(
        Command.UPDATE_REQUEST_CONTENT,
        (peer, buf, payload) -> jarUpdateHandler.handleRequestContent(peer));
    bare(
        Command.UPDATE_ANSWER_CONTENT,
        (peer, buf, payload) -> jarUpdateHandler.handleAnswerContent(buf, peer));
    bare(
        Command.ANDROID_UPDATE_REQUEST_TIMESTAMP,
        (peer, buf, payload) -> apkUpdateHandler.handleRequestTimestamp(peer));
    bare(
        Command.ANDROID_UPDATE_ANSWER_TIMESTAMP,
        (peer, buf, payload) -> apkUpdateHandler.handleAnswerTimestamp(buf, peer));
    bare(
        Command.ANDROID_UPDATE_REQUEST_CONTENT,
        (peer, buf, payload) -> apkUpdateHandler.handleRequestContent(peer));
    bare(
        Command.ANDROID_UPDATE_ANSWER_CONTENT,
        (peer, buf, payload) -> apkUpdateHandler.handleAnswerContent(buf, peer));

    // DHT (im.redpanda.kademlia)
    framed(Command.JOB_ACK, kademliaHandler::handleJobAck);
    framed(Command.KADEMLIA_GET, kademliaHandler::handleKademliaGet);
    framed(Command.KADEMLIA_STORE, kademliaHandler::handleKademliaStore);
    framed(Command.KADEMLIA_GET_ANSWER, kademliaHandler::handleKademliaGetAnswer);

    // Mailbox / garlic routing (im.redpanda.flaschenpost)
    framed(Command.FLASCHENPOST_PUT, flaschenpostHandler::handlePut);
    framed(Command.FLASCHENPOST_V2, (peer, payload) -> flaschenpostHandler.handleV2(payload));

    // Outbound V1 (im.redpanda.outbound). The *_RES commands and OUTBOUND_NOTIFY are only ever
    // written back to the client, never parsed here.
    framed(Command.OUTBOUND_REGISTER_OH_REQ, outboundHandler::handleRegister);
    framed(Command.OUTBOUND_FETCH_REQ, outboundHandler::handleFetch);
    framed(Command.OUTBOUND_REVOKE_OH_REQ, outboundHandler::handleRevoke);
    framed(Command.OUTBOUND_ACK_FETCH_REQ, outboundHandler::handleAckFetch);
    framed(Command.OUTBOUND_SUBSCRIBE_REQ, outboundHandler::handleSubscribe);
  }

  public void loopCommands(Peer peer, ByteBuffer readBuffer) {
    loopCommands(peer, readBuffer, false);
  }

  /**
   * @param callerOwnsBuffer {@code true} when the caller has exclusively claimed the buffer
   *     beforehand (T50 / REDPANDAJ-2EF ownership handoff in {@code
   *     ConnectionReaderThread.readConnection}: {@code peer.readBuffer} is {@code null} while this
   *     runs, so a handler-triggered {@link Peer#disconnect(String)} cannot return the buffer to
   *     the {@link ByteBufferPool} mid-loop). The buffer is then always compacted, keeping it in
   *     write mode for the caller's restore/return step — even on a handler exception or
   *     disconnect. With {@code false} (legacy wiring where the buffer is still referenced by
   *     {@code peer.readBuffer}) compact only happens while the field still points at this buffer:
   *     a handler-triggered disconnect already reset and returned the field's buffer
   *     (REDPANDAJ-2DR), so compacting the stale reference afterwards would corrupt whatever the
   *     pool's next borrower sees.
   */
  public void loopCommands(Peer peer, ByteBuffer readBuffer, boolean callerOwnsBuffer) {
    readBuffer.flip();

    int parsedBytesLocally = -1;

    // compact() must run even if a handler throws, otherwise the buffer state is left
    // inconsistent (flipped, position/limit not restored) and the connection keeps retrying
    // the same malformed packet. See the javadoc above for when compacting is safe.
    try {
      while (readBuffer.hasRemaining() && parsedBytesLocally != 0 && peer.isConnected()) {
        int newPosition = readBuffer.position();
        byte b = readBuffer.get();
        Log.put("command: " + b + " " + readBuffer, 200);
        parsedBytesLocally = parseCommand(b, readBuffer, peer);
        if (!peer.isConnected()) {
          return;
        }
        peer.lastCommand = b;
        newPosition += parsedBytesLocally;
        readBuffer.position(newPosition);
      }
    } finally {
      if (callerOwnsBuffer || peer.readBuffer == readBuffer) {
        readBuffer.compact();
      }
    }
  }

  /**
   * Parses one command off the connection buffer and runs its handler.
   *
   * @return the number of bytes consumed, or {@code 0} when the frame is not complete yet (the
   *     caller leaves the buffer untouched and retries after the next read) or the peer was
   *     disconnected
   */
  public int parseCommand(byte command, ByteBuffer readBuffer, Peer peer) {
    // Framed commands get their [len:4][payload] read here, before the handler runs, so a handler
    // never sees a partial frame: readMessage() resets the buffer and we report 0 consumed bytes
    // until the whole payload has arrived. Bare commands read whatever they need straight off the
    // buffer (the four update-timestamp answers read their 8 bytes themselves).
    byte[] payload = null;
    if (isPayloadCommand(command)) {
      payload = readMessage(readBuffer);
      if (payload == null) {
        return 0; // Not enough data yet
      }
    }

    CommandHandler handler = commandHandlers.get(command);
    if (handler != null) {
      try {
        return handler.handle(peer, readBuffer, payload);
      } catch (InvalidProtocolBufferException e) {
        logger.error("Failed to parse protobuf for command " + command, e);
        // A malformed payload must not desync the stream: skip the whole frame we already read,
        // so the next command byte is still at a frame boundary. Only framed commands can raise
        // this (protobuf parsing happens in their handlers), so payload is non-null in practice;
        // the bare fallback just skips the command byte.
        if (payload != null) {
          return 1 + 4 + payload.length;
        }
        return 1;
      }
    } else {
      // Protocol desync: the byte stream no longer aligns to a command boundary (observed as
      // command 0 right after another command, REDPANDAJ-2E0). A stream cipher cannot be resynced
      // mid-stream, and previously this only threw a RuntimeException that got logged while the
      // peer stayed connected and kept re-hitting the same desynced byte on every subsequent read.
      // Disconnect like a PeerProtocolException does so the peer reconnects and re-runs the
      // handshake, which resyncs the stream. loopCommands() sees the peer is no longer connected
      // and stops without touching the (already returned) buffer.
      logger.warn(
          "protocol desync: unknown command {} from peer (last cmd {}, lightClient {}),"
              + " disconnecting",
          command,
          peer.lastCommand,
          peer.isLightClient());
      peer.disconnect("unknown command " + command);
      return 0;
    }
  }

  /** Whether the command carries a length-prefixed payload; see {@link #framed}. */
  private boolean isPayloadCommand(byte command) {
    return framedCommands.contains(command);
  }

  private byte[] readMessage(ByteBuffer readBuffer) {
    if (readBuffer.remaining() < 4) {
      return null;
    }
    readBuffer.mark();
    int length = readBuffer.getInt();
    if (readBuffer.remaining() < length) {
      readBuffer.reset();
      return null;
    }
    byte[] bytes = new byte[length];
    readBuffer.get(bytes);
    return bytes;
  }
}
