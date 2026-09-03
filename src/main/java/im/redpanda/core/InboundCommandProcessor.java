package im.redpanda.core;

import static com.google.protobuf.ByteString.copyFrom;

import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.crypt.Utils;
import im.redpanda.flaschenpost.GarlicRouter;
import im.redpanda.flaschenpost.MailboxDepositPolicy;
import im.redpanda.jobs.Job;
import im.redpanda.jobs.KademliaInsertJob;
import im.redpanda.jobs.KademliaSearchJob;
import im.redpanda.jobs.KademliaSearchJobAnswerPeer;
import im.redpanda.kademlia.KadContent;
import im.redpanda.outbound.OutboundService;
import im.redpanda.outbound.v1.AckFetchRequest;
import im.redpanda.outbound.v1.FetchRequest;
import im.redpanda.outbound.v1.RegisterOhRequest;
import im.redpanda.outbound.v1.RevokeOhRequest;
import im.redpanda.outbound.v1.SubscribeRequest;
import im.redpanda.proto.*;
import im.redpanda.proto.FlaschenpostPut;
import im.redpanda.updater.ApkUpdateHandler;
import im.redpanda.updater.JarUpdateHandler;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Dispatches inbound commands of a peer connection: it owns the framing (command byte, optional
 * {@code [len:4][payload]} prefix, consumed-byte accounting, protocol-desync disconnect) and
 * nothing else — every command's meaning belongs to a domain handler.
 *
 * <p>T116 (DDD review 2026-08-31, P2 step 2/3) moved the handler bodies out; the wire behaviour is
 * unchanged. Domain map: peer list/liveness → this class (commands 1-4, the {@code core} context),
 * software distribution → {@link JarUpdateHandler}/{@link ApkUpdateHandler} (9-16), DHT → {@code
 * im.redpanda.kademlia} (120-123), mailbox → {@code OutboundService}/{@code MailboxDepositPolicy}
 * (141/142, 150-161), garlic routing → {@code GarlicRouter} (142).
 */
public class InboundCommandProcessor {
  private static final Logger logger = LogManager.getLogger();

  private final ServerContext serverContext;

  @FunctionalInterface
  private interface CommandHandler {
    int handle(Peer peer, ByteBuffer readBuffer, byte[] payload)
        throws InvalidProtocolBufferException;
  }

  private final java.util.Map<Byte, CommandHandler> commandHandlers = new java.util.HashMap<>();

  private final OutboundService outboundService;

  private final JarUpdateHandler jarUpdateHandler;
  private final ApkUpdateHandler apkUpdateHandler;

  public InboundCommandProcessor(ServerContext serverContext) {
    this.serverContext = serverContext;
    this.outboundService = serverContext.getOutboundService(); // Ensure ServerContext has this!
    this.jarUpdateHandler = new JarUpdateHandler(serverContext);
    this.apkUpdateHandler = new ApkUpdateHandler(serverContext);
    initializeHandlers();
  }

  private void initializeHandlers() {
    commandHandlers.put(Command.PING, (peer, buf, payload) -> handlePing(peer));
    commandHandlers.put(Command.PONG, (peer, buf, payload) -> handlePong(peer));
    commandHandlers.put(
        Command.REQUEST_PEERLIST, (peer, buf, payload) -> handleRequestPeerList(peer));

    // Outbound V1
    commandHandlers.put(
        Command.OUTBOUND_REGISTER_OH_REQ,
        (peer, buf, payload) -> {
          int len = (payload != null) ? payload.length : 0;
          outboundService.handleRegister(peer, RegisterOhRequest.parseFrom(payload));
          return 1 + 4 + len;
        });
    commandHandlers.put(
        Command.OUTBOUND_FETCH_REQ,
        (peer, buf, payload) -> {
          int len = (payload != null) ? payload.length : 0;
          outboundService.handleFetch(peer, FetchRequest.parseFrom(payload));
          return 1 + 4 + len;
        });
    commandHandlers.put(
        Command.OUTBOUND_REVOKE_OH_REQ,
        (peer, buf, payload) -> {
          int len = (payload != null) ? payload.length : 0;
          outboundService.handleRevoke(peer, RevokeOhRequest.parseFrom(payload));
          return 1 + 4 + len;
        });
    commandHandlers.put(
        Command.OUTBOUND_ACK_FETCH_REQ,
        (peer, buf, payload) -> {
          int len = (payload != null) ? payload.length : 0;
          outboundService.handleAckFetch(peer, AckFetchRequest.parseFrom(payload));
          return 1 + 4 + len;
        });
    // Connection-Notify (T38): opt-in subscribe. OUTBOUND_SUBSCRIBE_RES/OUTBOUND_NOTIFY are only
    // ever written back to the client, never parsed here.
    commandHandlers.put(
        Command.OUTBOUND_SUBSCRIBE_REQ,
        (peer, buf, payload) -> {
          int len = (payload != null) ? payload.length : 0;
          outboundService.handleSubscribe(peer, SubscribeRequest.parseFrom(payload));
          return 1 + 4 + len;
        });

    // Payload commands
    commandHandlers.put(
        Command.SEND_PEERLIST,
        (peer, buf, payload) -> handleSendPeerList(payload, peer) + 4 + payload.length);
    // N-UPDATER (T116): software distribution is its own bounded context; the dispatcher only
    // routes commands 9-16 into it.
    commandHandlers.put(
        Command.UPDATE_REQUEST_TIMESTAMP,
        (peer, buf, payload) -> jarUpdateHandler.handleRequestTimestamp(peer));
    commandHandlers.put(
        Command.UPDATE_ANSWER_TIMESTAMP,
        (peer, buf, payload) -> jarUpdateHandler.handleAnswerTimestamp(buf, peer));
    commandHandlers.put(
        Command.UPDATE_REQUEST_CONTENT,
        (peer, buf, payload) -> jarUpdateHandler.handleRequestContent(peer));
    commandHandlers.put(
        Command.UPDATE_ANSWER_CONTENT,
        (peer, buf, payload) -> jarUpdateHandler.handleAnswerContent(buf, peer));
    commandHandlers.put(
        Command.ANDROID_UPDATE_REQUEST_TIMESTAMP,
        (peer, buf, payload) -> apkUpdateHandler.handleRequestTimestamp(peer));
    commandHandlers.put(
        Command.ANDROID_UPDATE_ANSWER_TIMESTAMP,
        (peer, buf, payload) -> apkUpdateHandler.handleAnswerTimestamp(buf, peer));
    commandHandlers.put(
        Command.ANDROID_UPDATE_REQUEST_CONTENT,
        (peer, buf, payload) -> apkUpdateHandler.handleRequestContent(peer));
    commandHandlers.put(
        Command.ANDROID_UPDATE_ANSWER_CONTENT,
        (peer, buf, payload) -> apkUpdateHandler.handleAnswerContent(buf, peer));

    commandHandlers.put(
        Command.JOB_ACK,
        (peer, buf, payload) -> {
          handleJobAck(payload, peer);
          return 1 + 4 + payload.length;
        });
    commandHandlers.put(
        Command.KADEMLIA_GET,
        (peer, buf, payload) -> {
          handleKademliaGet(payload, peer);
          return 1 + 4 + payload.length;
        });
    commandHandlers.put(
        Command.KADEMLIA_STORE,
        (peer, buf, payload) -> {
          handleKademliaStore(payload, peer);
          return 1 + 4 + payload.length;
        });
    commandHandlers.put(
        Command.KADEMLIA_GET_ANSWER,
        (peer, buf, payload) -> {
          handleKademliaGetAnswer(payload, peer);
          return 1 + 4 + payload.length;
        });
    commandHandlers.put(
        Command.FLASCHENPOST_PUT,
        (peer, buf, payload) -> {
          handleFlaschenpostPut(payload, peer);
          return 1 + 4 + payload.length;
        });
    commandHandlers.put(
        Command.FLASCHENPOST_V2,
        (peer, buf, payload) -> {
          // MS04 multi-hop garlic relay: dedup, peel own layer or route toward next_hop
          GarlicRouter.handle(serverContext, payload);
          return 1 + 4 + payload.length;
        });
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

  public int parseCommand(byte command, ByteBuffer readBuffer, Peer peer) {
    // Commands with payload require reading length first for some handlers,
    // but the handler logic itself might not use it if it reads directly from
    // buffer (?)
    // Actually existing logic reads payload for specific commands before switch.
    // Let's preserve that logic or move it into handlers?
    // The previous logic pre-read payload for `isPayloadCommand`.
    // We should keep that pre-reading behaviour to be safe or refactor carefully.
    // The original code check `isPayloadCommand` then `readMessage`.
    // Let's keep that structure but pass the payload to the handler.

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
        // If payload was read, we can skip it.
        // The original code had specific fallback: return 1 + 4 + payload.length;
        // This assumes `payload` is not null if we are here and exception happened in a
        // payload handler.
        if (payload != null) {
          return 1 + 4 + payload.length;
        } else {
          // Should not happen for payload commands if logic matches, but strictly
          // speaking:
          return 1; // skip command byte? Or just return 0?
          // Original code only caught IPBE which comes from payload parsing.
          // So payload IS not null.
        }
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

  private boolean isPayloadCommand(byte command) {
    return command == Command.SEND_PEERLIST
        || command == Command.JOB_ACK
        || command == Command.KADEMLIA_GET
        || command == Command.KADEMLIA_STORE
        || command == Command.KADEMLIA_GET_ANSWER
        || command == Command.FLASCHENPOST_PUT
        || command == Command.FLASCHENPOST_V2
        || command == Command.OUTBOUND_REGISTER_OH_REQ
        || command == Command.OUTBOUND_FETCH_REQ
        || command == Command.OUTBOUND_REVOKE_OH_REQ
        || command == Command.OUTBOUND_ACK_FETCH_REQ
        || command == Command.OUTBOUND_SUBSCRIBE_REQ;
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

  private int handlePing(Peer peer) {
    Log.put("Received ping command", 200);
    if (!serverContext.getPeerList().contains(peer.getKademliaId())) {
      logger.error(
          "Got PING from node not in our peerlist, lets add it.... %s, id: %s"
              .formatted(peer, peer.getKademliaId()));
      serverContext.getPeerList().add(peer);
      return 0;
    }
    peer.enqueueCommand(Command.PONG);
    return 1;
  }

  private int handlePong(Peer peer) {
    Log.put("Received pong command", 200);
    peer.ping = (1 * peer.ping + (double) (System.currentTimeMillis() - peer.lastPinged)) / 2;
    peer.setLastPongReceived(System.currentTimeMillis());
    return 1;
  }

  private int handleRequestPeerList(Peer peer) {
    // T87: PeerList.snapshot() takes the read lock and releases it — the response is built and
    // written WITHOUT it. Holding it across the peer's writeBufferLock inverted the lock order
    // documented on PeerList: ConnectionHandler.setupConnection() holds a peer's writeBufferLock
    // and then takes the peer list WRITE lock, on the NIO selector thread. Reader thread and
    // selector thread could therefore each hold one of the two and wait for the other, and because
    // a ReentrantReadWriteLock is non-fair the selector's queued write request then blocked every
    // subsequent reader as well. That deadlocked a public seed node on 2026-07-29: the selector
    // stopped calling accept(), the listen backlog filled up and no client could connect any more.
    //
    // Snapshotting is also the pattern every other iteration site uses (NodeStore.addServerEdges,
    // PeerJobs.runOnce, Saver.savePeers, OhForwarder.selectNextPeer); the peers themselves were
    // never guarded by this lock, only the list structure, so nothing weakens by copying first.
    List<Peer> snapshot = serverContext.getPeerList().snapshot();

    var builder = SendPeerList.newBuilder();
    for (Peer peerToCheck : snapshot) {
      if (peerToCheck.ip == null) {
        continue;
      }
      // Same predicate as on the ingest path, with the recipient as the "other side": do not
      // hand a local-only address to a peer outside that network, and do not pass on entries
      // that carry no dialable port. Without this we amplify exactly what we refuse to accept.
      if (!Utils.isPlausibleAdvertisedAddress(peerToCheck.ip, peerToCheck.getPort(), peer.ip)) {
        continue;
      }
      var peerBuilder =
          PeerInfoProto.newBuilder().setIp(peerToCheck.ip).setPort(peerToCheck.getPort());
      if (peerToCheck.getNodeId() != null && peerToCheck.getNodeId().hasKey()) {
        peerBuilder.setNodeId(
            NodeIdProto.newBuilder()
                .setPublicKeyBytes(copyFrom(peerToCheck.getNodeId().exportPublic()))
                .build());
        // MS04: explicit X25519 key so light clients can pick garlic hops directly
        peerBuilder.setEncryptionPublicKey(
            copyFrom(peerToCheck.getNodeId().getEncryptionPubKey().getEncoded()));
      }
      builder.addPeers(peerBuilder.build());
    }
    byte[] data = builder.build().toByteArray();
    peer.enqueueFrame(Command.SEND_PEERLIST, data);
    return 1;
  }

  private int handleSendPeerList(byte[] bytesForPeerList, Peer peer)
      throws InvalidProtocolBufferException {
    SendPeerList sendPeerList = SendPeerList.parseFrom(bytesForPeerList);
    for (PeerInfoProto peerProto : sendPeerList.getPeersList()) {
      if (serverContext.getPeerList().size() >= Settings.MAX_PEERLIST_SIZE) {
        Log.put("peer list is full, ignoring the rest of the gossiped peer list", 40);
        break;
      }
      NodeId nodeId = null;
      if (peerProto.hasNodeId()) {
        try {
          nodeId = NodeId.importPublic(peerProto.getNodeId().getPublicKeyBytes().toByteArray());
        } catch (IllegalArgumentException e) {
          // malformed or legacy (pre-MS03) key in the peer list — skip this entry
          continue;
        }
      }
      String ip = peerProto.getIp();
      int port = peerProto.getPort();
      // Peer-list gossip is unauthenticated: everything below this point comes from the peer and
      // nothing above verifies it. Reject entries that the advertising peer cannot plausibly know
      // about — otherwise any peer can steer us into dialling loopback, the local LAN or a
      // portless address, and we then spread those entries further.
      if (!Utils.isPlausibleAdvertisedAddress(ip, port, peer.ip)) {
        Log.put("ignoring implausible peer list entry " + ip + ":" + port + " from " + peer.ip, 40);
        continue;
      }
      if (port == serverContext.getPort() && Utils.isOwnHostAddress(ip)) {
        Log.put("ignoring peer list entry that points back at us: " + ip + ":" + port, 40);
        continue;
      }
      if (nodeId != null) {
        if (nodeId.getKademliaId().equals(serverContext.getNonce())) {
          Log.put("found ourselves in the peerlist", 80);
          continue;
        }
        Peer newPeer = new Peer(ip, port, nodeId);
        var byKademliaId = Node.getByKademliaId(serverContext, nodeId.getKademliaId());
        if (byKademliaId != null) {
          byKademliaId.addConnectionPoint(ip, port);
        } else {
          new Node(serverContext, nodeId);
        }
        serverContext.getPeerList().add(newPeer);
      } else {
        serverContext.getPeerList().add(new Peer(ip, port));
      }
    }
    return 1; // Base command length, payload length added by caller
  }

  private void handleJobAck(byte[] payload, Peer peer) throws InvalidProtocolBufferException {
    JobAck ackMsg = JobAck.parseFrom(payload);
    int jobId = ackMsg.getJobId();
    var runningJob = Job.getRunningJob(jobId);
    if (runningJob instanceof KademliaInsertJob job) {
      job.ack(peer);
      System.out.println("ACK from peer: " + peer.getNodeId().toString());
    }
  }

  private void handleKademliaGet(byte[] payload, Peer peer) throws InvalidProtocolBufferException {
    KademliaGet getMsg = KademliaGet.parseFrom(payload);
    int jobId = getMsg.getJobId();
    var searchedId = new KademliaId(getMsg.getSearchedId().getKeyBytes().toByteArray());
    var kadContent = serverContext.getKadStoreManager().get(searchedId);
    if (kadContent != null) {
      var answerMsg =
          KademliaGetAnswer.newBuilder()
              .setAckId(jobId)
              .setTimestamp(kadContent.getTimestamp())
              .setPublicKey(copyFrom(kadContent.getPubkey()))
              .setContent(copyFrom(kadContent.getContent()))
              .setSignature(copyFrom(kadContent.getSignature()))
              .build();
      peer.enqueueFrame(Command.KADEMLIA_GET_ANSWER, answerMsg.toByteArray());
    } else {
      new KademliaSearchJobAnswerPeer(serverContext, searchedId, peer, jobId).start();
    }
  }

  private void handleKademliaStore(byte[] payload, Peer peer)
      throws InvalidProtocolBufferException {
    KademliaStore storeMsg = KademliaStore.parseFrom(payload);
    int jobId = storeMsg.getJobId();
    var kadContent =
        new KadContent(
            storeMsg.getTimestamp(),
            storeMsg.getPublicKey().toByteArray(),
            storeMsg.getContent().toByteArray(),
            storeMsg.getSignature().toByteArray());
    if (kadContent.verify()) {
      serverContext.getKadStoreManager().put(kadContent);
      if (jobId != 0) {
        var ackMsg = JobAck.newBuilder().setJobId(jobId).build();
        peer.enqueueFrame(Command.JOB_ACK, ackMsg.toByteArray());
      }
    } else {
      logger.error("Kademlia content verification failed!");
    }
  }

  private void handleKademliaGetAnswer(byte[] payload, Peer peer)
      throws InvalidProtocolBufferException {
    KademliaGetAnswer answerMsg = KademliaGetAnswer.parseFrom(payload);
    var kadContent =
        new KadContent(
            answerMsg.getTimestamp(),
            answerMsg.getPublicKey().toByteArray(),
            answerMsg.getContent().toByteArray(),
            answerMsg.getSignature().toByteArray());
    if (kadContent.verify()) {
      var byId = Job.getRunningJob(answerMsg.getAckId());
      if (byId instanceof KademliaSearchJob job) {
        job.ack(kadContent, peer);
      }
    } else {
      logger.error("Kademlia content verification failed!");
    }
  }

  /**
   * Parses the FLASCHENPOST_PUT frame and hands it to the mailbox domain.
   *
   * <p>The deposit/forward/R-ACK policy that used to live here (incl. the legacy garlic-destination
   * deposit) is owned by {@link MailboxDepositPolicy} since the DDD review 2026-08-31 — the wire
   * parser only parses and delegates.
   */
  private void handleFlaschenpostPut(byte[] payload, Peer peer)
      throws InvalidProtocolBufferException {
    MailboxDepositPolicy.handlePut(
        serverContext, outboundService, FlaschenpostPut.parseFrom(payload), peer);
  }
}
