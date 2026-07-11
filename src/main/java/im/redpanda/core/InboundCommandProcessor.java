package im.redpanda.core;

import static com.google.protobuf.ByteString.copyFrom;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.flaschenpost.GMParser;
import im.redpanda.flaschenpost.GarlicRouter;
import im.redpanda.flaschenpost.OhForwarder;
import im.redpanda.flaschenpost.ReturnPath;
import im.redpanda.flaschenpost.RoutingAckSender;
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
import im.redpanda.proto.*;
import im.redpanda.proto.FlaschenpostPut;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Parses and processes inbound commands for a peer connection. Extracted from
 * ConnectionReaderThread to improve SRP and testability.
 */
public class InboundCommandProcessor {
  private static final Logger logger = LogManager.getLogger();

  /** Ed25519 signatures are fixed-size (64 bytes, no DER framing). */
  public static final int SIGNATURE_LEN = NodeId.SIGNATURE_LEN;

  /** Reject update timestamps further in the future than this (clock-skew / spoofing guard). */
  private static final long MAX_FUTURE_SKEW_MS = 24L * 60 * 60 * 1000;

  /**
   * Invoked to apply an installed update. Default restarts the JVM; tests replace this with a
   * counter so the positive-path test never actually exits the test JVM.
   */
  static Runnable restartAction = () -> System.exit(0);

  /** System property overriding {@link #updateJarPath()}; used by tests to avoid CWD sharing. */
  private static final String JAR_PATH_PROPERTY = "redpanda.update.jar.path";

  /** System property overriding {@link #updateApkPath()}; used by tests to avoid CWD sharing. */
  private static final String APK_PATH_PROPERTY = "redpanda.update.apk.path";

  /**
   * Path of the local redpanda.jar that gets uploaded to peers requesting it. Defaults to the usual
   * seed-node vs. client layout, overridable via {@value #JAR_PATH_PROPERTY} (tests only, so
   * Surefire forks sharing the working directory don't collide).
   */
  static Path updateJarPath() {
    String override = System.getProperty(JAR_PATH_PROPERTY);
    if (override != null) {
      return Path.of(override);
    }
    return Settings.isSeedNode() ? Path.of("target/redpanda.jar") : Path.of("redpanda.jar");
  }

  /**
   * Path of the local android.apk that gets uploaded to peers requesting it / that a received
   * update is written to. Defaults to {@link ConnectionReaderThread#ANDROID_UPDATE_FILE},
   * overridable via {@value #APK_PATH_PROPERTY} (tests only).
   */
  static Path updateApkPath() {
    String override = System.getProperty(APK_PATH_PROPERTY);
    if (override != null) {
      return Path.of(override);
    }
    return Path.of(ConnectionReaderThread.ANDROID_UPDATE_FILE);
  }

  private final ServerContext serverContext;

  @FunctionalInterface
  private interface CommandHandler {
    int handle(Peer peer, ByteBuffer readBuffer, byte[] payload)
        throws InvalidProtocolBufferException;
  }

  private final java.util.Map<Byte, CommandHandler> commandHandlers = new java.util.HashMap<>();

  private final OutboundService outboundService;

  public InboundCommandProcessor(ServerContext serverContext) {
    this.serverContext = serverContext;
    this.outboundService = serverContext.getOutboundService(); // Ensure ServerContext has this!
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

    // Payload commands
    commandHandlers.put(
        Command.SEND_PEERLIST,
        (peer, buf, payload) -> handleSendPeerList(payload, peer) + 4 + payload.length);
    commandHandlers.put(
        Command.UPDATE_REQUEST_TIMESTAMP,
        (peer, buf, payload) -> handleUpdateRequestTimestamp(peer));
    commandHandlers.put(
        Command.UPDATE_ANSWER_TIMESTAMP,
        (peer, buf, payload) -> handleUpdateAnswerTimestamp(buf, peer));
    commandHandlers.put(
        Command.UPDATE_REQUEST_CONTENT, (peer, buf, payload) -> handleUpdateRequestContent(peer));
    commandHandlers.put(
        Command.UPDATE_ANSWER_CONTENT,
        (peer, buf, payload) -> handleUpdateAnswerContent(buf, peer));
    commandHandlers.put(
        Command.ANDROID_UPDATE_REQUEST_TIMESTAMP,
        (peer, buf, payload) -> handleAndroidUpdateRequestTimestamp(peer));
    commandHandlers.put(
        Command.ANDROID_UPDATE_ANSWER_TIMESTAMP,
        (peer, buf, payload) -> handleAndroidUpdateAnswerTimestamp(buf, peer));
    commandHandlers.put(
        Command.ANDROID_UPDATE_REQUEST_CONTENT,
        (peer, buf, payload) -> handleAndroidUpdateRequestContent(peer));
    commandHandlers.put(
        Command.ANDROID_UPDATE_ANSWER_CONTENT,
        (peer, buf, payload) -> handleAndroidUpdateAnswerContent(buf, peer));

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
    readBuffer.flip();

    int parsedBytesLocally = -1;

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

    readBuffer.compact();
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
      throw new RuntimeException(
          "Got unknown command from peer: "
              + command
              + " last cmd: "
              + peer.lastCommand
              + " lightClient: "
              + peer.isLightClient());
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
        || command == Command.OUTBOUND_ACK_FETCH_REQ;
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
    peer.getWriteBufferLock().lock();
    try {
      peer.writeBuffer.put(Command.PONG);
    } finally {
      peer.getWriteBufferLock().unlock();
    }
    return 1;
  }

  private int handlePong(Peer peer) {
    Log.put("Received pong command", 200);
    peer.ping = (1 * peer.ping + (double) (System.currentTimeMillis() - peer.lastPinged)) / 2;
    peer.setLastPongReceived(System.currentTimeMillis());
    return 1;
  }

  private int handleRequestPeerList(Peer peer) {
    serverContext.getPeerList().getReadWriteLock().readLock().lock();
    try {
      var builder = SendPeerList.newBuilder();
      for (Peer peerToCheck : serverContext.getPeerList().getPeerArrayList()) {
        if (peerToCheck.ip == null) {
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
      peer.getWriteBufferLock().lock();
      try {
        peer.writeBuffer.put(Command.SEND_PEERLIST);
        peer.writeBuffer.putInt(data.length);
        peer.writeBuffer.put(data);
        peer.setWriteBufferFilled();
      } finally {
        peer.getWriteBufferLock().unlock();
      }
    } finally {
      serverContext.getPeerList().getReadWriteLock().readLock().unlock();
    }
    return 1;
  }

  private int handleSendPeerList(byte[] bytesForPeerList, Peer peer)
      throws InvalidProtocolBufferException {
    SendPeerList sendPeerList = SendPeerList.parseFrom(bytesForPeerList);
    for (PeerInfoProto peerProto : sendPeerList.getPeersList()) {
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
      if (nodeId != null) {
        if (nodeId.getKademliaId().equals(serverContext.getNonce())) {
          Log.put("found ourselves in the peerlist", 80);
          continue;
        }
        if (ip == null) {
          System.out.println("found a peer with ip null...");
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

  private int handleUpdateRequestTimestamp(Peer peer) {
    ByteBuffer writeBuffer = peer.getWriteBuffer();
    peer.writeBufferLock.lock();
    try {
      writeBuffer.put(Command.UPDATE_ANSWER_TIMESTAMP);
      writeBuffer.putLong(serverContext.getLocalSettings().getUpdateTimestamp());
    } finally {
      peer.writeBufferLock.unlock();
    }
    peer.setWriteBufferFilled();
    return 1;
  }

  private int handleUpdateAnswerTimestamp(ByteBuffer readBuffer, Peer peer) {
    if (8 > readBuffer.remaining()) {
      return 0;
    }
    long othersTimestamp = readBuffer.getLong();
    if (othersTimestamp > System.currentTimeMillis() + MAX_FUTURE_SKEW_MS) {
      logger.warn("rejecting update timestamp too far in the future: {}", othersTimestamp);
      return 1 + 8;
    }
    long floor =
        Math.max(
            serverContext.getLocalSettings().getUpdateTimestamp(), Updater.MIN_UPDATE_TIMESTAMP_MS);
    if (othersTimestamp < serverContext.getLocalSettings().getUpdateTimestamp()) {
      System.out.println("WARNING: peer has outdated redPandaj version! " + peer.getNodeId());
    }
    if (othersTimestamp > floor && Settings.isLoadUpdates()) {
      Runnable runnable =
          () -> {
            ConnectionReaderThread.updateDownloadLock.lock();
            try {
              System.out.println("our version is outdated, we try to download it from this peer!");
              peer.writeBufferLock.lock();
              peer.getWriteBuffer().put(Command.UPDATE_REQUEST_CONTENT);
              peer.writeBufferLock.unlock();
              peer.setWriteBufferFilled();
              try {
                Thread.sleep(60000);
              } catch (InterruptedException ignored) {
              }
            } finally {
              System.out.println("we can now download it from another peer...");
              ConnectionReaderThread.updateDownloadLock.unlock();
            }
          };
      Server.threadPool.submit(runnable);
    }
    return 1 + 8;
  }

  private int handleUpdateRequestContent(Peer peer) {
    if (serverContext.getLocalSettings().getUpdateTimestamp() == -1) {
      return 1;
    }
    if (serverContext.getLocalSettings().getUpdateSignature() == null) {
      System.out.println(
          "we dont have an official signature to upload that update to other peers!");
      return 1;
    }
    Runnable runnable =
        () -> {
          ConnectionReaderThread.updateUploadLock.acquireUninterruptibly();
          try {
            try {
              Thread.sleep(200);
            } catch (InterruptedException ignored) {
            }
            Path path = updateJarPath();
            try {
              System.out.println("we send the update to a peer!");
              byte[] data = Files.readAllBytes(path);
              ByteBuffer a =
                  ByteBuffer.allocate(
                      1
                          + 8
                          + 4
                          + serverContext.getLocalSettings().getUpdateSignature().length
                          + data.length);
              a.put(Command.UPDATE_ANSWER_CONTENT);
              a.putLong(serverContext.getLocalSettings().getUpdateTimestamp());
              a.putInt(data.length);
              a.put(serverContext.getLocalSettings().getUpdateSignature());
              a.put(data);
              a.flip();
              peer.writeBufferLock.lock();
              try {
                if (peer.writeBuffer.remaining() < a.remaining()) {
                  ByteBuffer allocate =
                      ByteBuffer.allocate(
                          peer.writeBuffer.capacity() + a.remaining() + 1024 * 1024 * 10);
                  peer.writeBuffer.flip();
                  allocate.put(peer.writeBuffer);
                  peer.writeBuffer = allocate;
                }
                peer.writeBuffer.put(a.array());
                peer.setWriteBufferFilled();
              } finally {
                peer.writeBufferLock.unlock();
              }
            } catch (FileNotFoundException e) {
              Log.sentry(e);
              e.printStackTrace();
            } catch (IOException e) {
              Log.sentry(e);
              Log.sentry(e);
            }
          } finally {
            ConnectionReaderThread.updateUploadLock.release();
          }
        };
    ConnectionReaderThread.threadPool.submit(runnable);
    return 1;
  }

  private int handleUpdateAnswerContent(ByteBuffer readBuffer, Peer peer) {
    if (8 + 4 + SIGNATURE_LEN > readBuffer.remaining()) {
      return 0;
    }
    long othersTimestamp = readBuffer.getLong();
    int toReadBytes = readBuffer.getInt();
    byte[] signatureBytes = new byte[SIGNATURE_LEN];
    readBuffer.get(signatureBytes);
    int lenOfSignature = signatureBytes.length;
    if (toReadBytes < 0) {
      // Network-controlled length: a negative value is a protocol violation and would
      // throw NegativeArraySizeException below (reader thread DoS).
      logger.warn("negative update content length from peer, disconnecting: {}", toReadBytes);
      peer.disconnect("negative update content length");
      return 0;
    }
    if (toReadBytes > readBuffer.remaining()) {
      return 0;
    }
    byte[] data = new byte[toReadBytes];
    readBuffer.get(data);
    int consumedBytes = 1 + 8 + 4 + lenOfSignature + data.length;
    if (othersTimestamp > System.currentTimeMillis() + MAX_FUTURE_SKEW_MS) {
      logger.warn("rejecting update: timestamp too far in the future: {}", othersTimestamp);
      return consumedBytes;
    }
    long floor =
        Math.max(
            serverContext.getLocalSettings().getUpdateTimestamp(), Updater.MIN_UPDATE_TIMESTAMP_MS);
    if (othersTimestamp > floor) {

      // Verify signature before writing anything
      NodeId publicUpdaterKey = Updater.getPublicUpdaterKey();
      if (publicUpdaterKey == null) {
        System.out.println("No public updater key available, cannot verify update.");
        return consumedBytes;
      }

      ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
      toHash.putLong(othersTimestamp);
      toHash.put(data);

      if (!publicUpdaterKey.verify(toHash.array(), signatureBytes)) {
        System.out.println("Update verification failed! Signature invalid.");
        return consumedBytes;
      }

      try (FileOutputStream fos = new FileOutputStream("tmp_redpanda.jar")) {
        fos.write(data);
      } catch (IOException e) {
        Log.sentry(e);
        return consumedBytes;
      }

      try {
        // Install the update
        // Save to 'update' file so the shell script can pick it up and restart
        Files.move(
            Path.of("tmp_redpanda.jar"), Path.of("update"), StandardCopyOption.REPLACE_EXISTING);

        // Update local settings
        serverContext.getLocalSettings().setUpdateTimestamp(othersTimestamp);
        serverContext.getLocalSettings().setUpdateSignature(signatureBytes);
        serverContext.getLocalSettings().save(serverContext.getPort());

        System.out.println(
            "Update successfully verified and saved to 'update'. New timestamp: "
                + othersTimestamp);
        System.out.println("Stopping server to apply update...");

        // Exit asynchronously to allow current method to return and log to be written
        // Exit asynchronously to allow current method to return and log to be written
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    Thread.sleep(2000);
                  } catch (InterruptedException e) {
                  }
                  restartAction.run();
                });

      } catch (IOException e) {
        Log.sentry(e);
        System.out.println("Failed to install update: " + e.getMessage());
      }
    }
    return consumedBytes;
  }

  private int handleAndroidUpdateRequestTimestamp(Peer peer) {
    File file = updateApkPath().toFile();
    if (!file.exists()) {
      return 1;
    }
    peer.writeBufferLock.lock();
    peer.getWriteBuffer().put(Command.ANDROID_UPDATE_ANSWER_TIMESTAMP);
    peer.getWriteBuffer().putLong(serverContext.getLocalSettings().getUpdateAndroidTimestamp());
    peer.writeBufferLock.unlock();
    peer.setWriteBufferFilled();
    return 1;
  }

  private int handleAndroidUpdateAnswerTimestamp(ByteBuffer readBuffer, Peer peer) {
    if (8 > readBuffer.remaining()) {
      return 0;
    }
    long othersTimestamp = readBuffer.getLong();
    if (othersTimestamp > System.currentTimeMillis() + MAX_FUTURE_SKEW_MS) {
      logger.warn("rejecting android update timestamp too far in the future: {}", othersTimestamp);
      return 1 + 8;
    }
    long floor =
        Math.max(
            serverContext.getLocalSettings().getUpdateAndroidTimestamp(),
            Updater.MIN_UPDATE_TIMESTAMP_MS);
    Log.put(
        "Update found from: "
            + new Date(othersTimestamp)
            + " our version is from: "
            + new Date(serverContext.getLocalSettings().getUpdateAndroidTimestamp()),
        70);
    if (othersTimestamp < serverContext.getLocalSettings().getUpdateAndroidTimestamp()) {
      System.out.println("WARNING: peer has outdated android.apk version! " + peer.getNodeId());
    }
    if (othersTimestamp > floor) {
      Runnable runnable =
          () -> {
            ConnectionReaderThread.updateUploadLock.acquireUninterruptibly();
            try {
              if (othersTimestamp <= serverContext.getLocalSettings().getUpdateAndroidTimestamp()) {
                return;
              }
              System.out.println(
                  "our android.apk version is outdated, we try to download it from this peer!");
              peer.writeBufferLock.lock();
              peer.writeBuffer.put(Command.ANDROID_UPDATE_REQUEST_CONTENT);
              peer.writeBufferLock.unlock();
              peer.setWriteBufferFilled();
              try {
                Thread.sleep(60000);
              } catch (InterruptedException ignored) {
              }
            } finally {
              System.out.println("we can now download it from another peer...");
              ConnectionReaderThread.updateUploadLock.release();
            }
          };
      InboundCommandProcessor.this.serverContext.getNodeStore();
      ConnectionReaderThread.threadPool.submit(runnable);
    }
    return 1 + 8;
  }

  private int handleAndroidUpdateRequestContent(Peer peer) {
    if (serverContext.getLocalSettings().getUpdateAndroidSignature() == null) {
      System.out.println(
          "we dont have an official signature to upload that android.apk update to other peers!");
      return 1;
    }
    Runnable runnable =
        () -> {
          ConnectionReaderThread.updateUploadLock.acquireUninterruptibly();
          try {
            try {
              Thread.sleep(200);
            } catch (InterruptedException ignored) {
            }
            Path path = updateApkPath();
            try {
              NodeId publicUpdaterKey = Updater.getPublicUpdaterKey();
              if (publicUpdaterKey == null) {
                System.out.println("No public updater key available, cannot verify update.");
                return;
              }
              byte[] data = Files.readAllBytes(path);
              ByteBuffer bytesToHash = ByteBuffer.allocate(8 + data.length);
              bytesToHash.putLong(serverContext.getLocalSettings().getUpdateAndroidTimestamp());
              bytesToHash.put(data);
              boolean verify =
                  publicUpdaterKey.verify(
                      bytesToHash.array(),
                      serverContext.getLocalSettings().getUpdateAndroidSignature());
              if (!verify) {
                System.out.println(
                    "################################ update not verified "
                        + serverContext.getLocalSettings().getUpdateAndroidTimestamp());
                return;
              }
              System.out.println("we send the android.apk update to a peer!");
              byte[] androidSignature =
                  serverContext.getLocalSettings().getUpdateAndroidSignature();
              ByteBuffer a = ByteBuffer.allocate(1 + 8 + 4 + androidSignature.length + data.length);
              a.put(Command.ANDROID_UPDATE_ANSWER_CONTENT);
              a.putLong(serverContext.getLocalSettings().getUpdateAndroidTimestamp());
              a.putInt(data.length);
              a.put(androidSignature);
              a.put(data);
              a.flip();
              peer.writeBufferLock.lock();
              try {
                if (peer.writeBuffer.remaining() < a.remaining()) {
                  ByteBuffer allocate =
                      ByteBuffer.allocate(
                          peer.writeBuffer.capacity() + a.remaining() + 1024 * 1024 * 10);
                  peer.writeBuffer.flip();
                  allocate.put(peer.writeBuffer);
                  peer.writeBuffer = allocate;
                }
                peer.writeBuffer.put(a.array());
                peer.setWriteBufferFilled();
              } finally {
                peer.writeBufferLock.unlock();
              }
              int cnt = 0;
              while (cnt < 6) {
                cnt++;
                try {
                  Thread.sleep(10000);
                } catch (InterruptedException ignored) {
                }
                peer.writeBufferLock.lock();
                try {
                  if (!peer.isConnected()
                      || (peer.writeBuffer.position() == 0
                          && peer.writeBufferCrypted.position() == 0)) {
                    break;
                  }
                } finally {
                  peer.writeBufferLock.unlock();
                }
                System.out.println("peer still downloading...");
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
          } finally {
            ConnectionReaderThread.updateUploadLock.release();
          }
        };
    ConnectionReaderThread.threadPool.submit(runnable);
    return 1;
  }

  private int handleAndroidUpdateAnswerContent(ByteBuffer readBuffer, Peer peer) {
    if (8 + 4 + SIGNATURE_LEN > readBuffer.remaining()) {
      return 0;
    }
    long othersTimestamp = readBuffer.getLong();
    int toReadBytes = readBuffer.getInt();
    byte[] signature = new byte[SIGNATURE_LEN];
    readBuffer.get(signature);
    int signatureLen = signature.length;
    if (toReadBytes < 0) {
      // Network-controlled length: a negative value is a protocol violation and would
      // throw NegativeArraySizeException below (reader thread DoS).
      logger.warn(
          "negative android update content length from peer, disconnecting: {}", toReadBytes);
      peer.disconnect("negative android update content length");
      return 0;
    }
    if (toReadBytes > readBuffer.remaining()) {
      return 0;
    }
    byte[] data = new byte[toReadBytes];
    readBuffer.get(data);
    int consumedBytes = 1 + 8 + 4 + signatureLen + data.length;
    if (othersTimestamp > System.currentTimeMillis() + MAX_FUTURE_SKEW_MS) {
      logger.warn("rejecting android update: timestamp too far in the future: {}", othersTimestamp);
      return consumedBytes;
    }
    long floor =
        Math.max(
            serverContext.getLocalSettings().getUpdateAndroidTimestamp(),
            Updater.MIN_UPDATE_TIMESTAMP_MS);
    if (othersTimestamp > floor) {

      // Verify signature
      NodeId publicUpdaterKey = Updater.getPublicUpdaterKey();
      if (publicUpdaterKey == null) {
        System.out.println("No public updater key available, cannot verify android update.");
        return consumedBytes;
      }

      ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
      toHash.putLong(othersTimestamp);
      toHash.put(data);

      if (!publicUpdaterKey.verify(toHash.array(), signature)) {
        System.out.println("Android update verification failed! Signature invalid.");
        return consumedBytes;
      }

      try (FileOutputStream fos = new FileOutputStream(updateApkPath().toFile())) {
        fos.write(data);
      } catch (IOException e) {
        e.printStackTrace();
      }
      serverContext.getLocalSettings().setUpdateAndroidTimestamp(othersTimestamp);
      serverContext.getLocalSettings().setUpdateAndroidSignature(signature);
      serverContext.getLocalSettings().save(serverContext.getPort());
    }
    return consumedBytes;
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
      peer.getWriteBufferLock().lock();
      try {
        var answerMsg =
            KademliaGetAnswer.newBuilder()
                .setAckId(jobId)
                .setTimestamp(kadContent.getTimestamp())
                .setPublicKey(copyFrom(kadContent.getPubkey()))
                .setContent(copyFrom(kadContent.getContent()))
                .setSignature(copyFrom(kadContent.getSignature()))
                .build();
        byte[] answerData = answerMsg.toByteArray();
        peer.getWriteBuffer().put(Command.KADEMLIA_GET_ANSWER);
        peer.getWriteBuffer().putInt(answerData.length);
        peer.getWriteBuffer().put(answerData);
        peer.setWriteBufferFilled();
      } finally {
        peer.getWriteBufferLock().unlock();
      }
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
        peer.getWriteBufferLock().lock();
        try {
          var ackMsg = JobAck.newBuilder().setJobId(jobId).build();
          byte[] ackData = ackMsg.toByteArray();
          peer.getWriteBuffer().put(Command.JOB_ACK);
          peer.getWriteBuffer().putInt(ackData.length);
          peer.getWriteBuffer().put(ackData);
          peer.setWriteBufferFilled();
        } finally {
          peer.getWriteBufferLock().unlock();
        }
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

  private void handleFlaschenpostPut(byte[] payload, Peer peer)
      throws InvalidProtocolBufferException {
    FlaschenpostPut putMsg = FlaschenpostPut.parseFrom(payload);
    byte[] content = putMsg.getContent().toByteArray();

    // MS01: Direct OH routing via explicit oh_id field.
    // MS02b: this path is authoritative — a packet with an explicit oh_id is deposited or
    // dropped (with an opt-in status response) and never falls through to the legacy garlic
    // parsing, which would misinterpret raw client payloads as GarlicMessages.
    ByteString ohIdBytes = putMsg.getOhId();
    if (!ohIdBytes.isEmpty() && outboundService != null) {
      // Validate OH id length before converting to a byte array to avoid large allocations
      if (ohIdBytes.size() != KademliaId.ID_LENGTH_BYTES) {
        logger.warn(
            "Received FlaschenpostPut with invalid oh_id length: {}, expected {}",
            ohIdBytes.size(),
            KademliaId.ID_LENGTH_BYTES);
        respondToDeposit(peer, putMsg, im.redpanda.outbound.v1.Status.BAD_REQUEST);
        return;
      }
      byte[] ohId = ohIdBytes.toByteArray();
      // Pre-check the size limit before any deposit/forward decision: an oversized payload is
      // rejected by every host node anyway, so forwarding it (and answering OK) would only waste
      // hops and mislead the sender.
      if (content.length > im.redpanda.outbound.OutboundMailboxStore.MAX_ITEM_BYTES) {
        respondToDeposit(peer, putMsg, im.redpanda.outbound.v1.Status.BAD_REQUEST);
        return;
      }
      // MS05: a reverse-garlic session tag arrives here when the final garlic hop was not the
      // OH host and forwarded the tagged deliver (OhForwarder). Empty for direct deposits.
      // Validate the size on the ByteString before materializing the array (cf. oh_id above).
      ByteString sessionTagBytes = putMsg.getSessionTag();
      if (sessionTagBytes.size() != 0
          && sessionTagBytes.size() != OutboundService.SESSION_TAG_BYTES) {
        respondToDeposit(peer, putMsg, im.redpanda.outbound.v1.Status.BAD_REQUEST);
        return;
      }
      byte[] sessionTag = sessionTagBytes.toByteArray();
      // MS06: a return-path block arrives here when a CMD_DELIVER_ACKED deliver was forwarded
      // by a non-host final garlic hop (OhForwarder, like the MS05 session tag). Structurally
      // invalid blocks reject the deposit like an invalid session tag.
      ByteString returnPathBytes = putMsg.getReturnPath();
      ReturnPath returnPath = null;
      if (!returnPathBytes.isEmpty()) {
        if (returnPathBytes.size() > ReturnPath.MAX_SERIALIZED_LEN) {
          respondToDeposit(peer, putMsg, im.redpanda.outbound.v1.Status.BAD_REQUEST);
          return;
        }
        returnPath = ReturnPath.parseExact(returnPathBytes.toByteArray());
        if (returnPath == null) {
          respondToDeposit(peer, putMsg, im.redpanda.outbound.v1.Status.BAD_REQUEST);
          return;
        }
      }
      OutboundService.DepositResult result =
          outboundService.depositMessage(ohId, content, sessionTag);
      if (result == OutboundService.DepositResult.NOT_FOUND) {
        // MS02b: not our OH — forward toward the host node (resolved via the DHT announce),
        // preserving the oh_id (and MS05 session tag / MS06 return path) on every hop.
        // Best-effort: OK means "accepted for forwarding".
        boolean accepted =
            OhForwarder.forward(
                serverContext,
                ohId,
                content,
                putMsg.getHopCount(),
                sessionTag,
                returnPathBytes.isEmpty() ? null : returnPathBytes.toByteArray());
        if (!accepted && returnPath != null) {
          // final station for this packet (hop limit) and the OH is unknown here — tell the
          // sender the handle could not be resolved instead of leaving it to the timeout
          RoutingAckSender.send(serverContext, returnPath, RoutingAckSender.STATUS_HANDLE_EXPIRED);
        }
        respondToDeposit(
            peer,
            putMsg,
            accepted
                ? im.redpanda.outbound.v1.Status.OK
                : im.redpanda.outbound.v1.Status.NOT_FOUND);
        return;
      }
      if (result != OutboundService.DepositResult.DEPOSITED) {
        logger.debug("FlaschenpostPut deposit not stored: {}", result);
      }
      if (returnPath != null) {
        // MS06: this node made the final deposit decision — send the R-ACK
        RoutingAckSender.send(serverContext, returnPath, RoutingAckSender.statusFor(result));
      }
      respondToDeposit(peer, putMsg, OutboundService.depositResultToStatus(result));
      return;
    }

    // Legacy: Try to route via GarlicMessage destination header
    if (tryDepositToLocalOh(content)) {
      return;
    }

    GMParser.parse(serverContext, content);
  }

  /**
   * Sends the MS02b deposit status response, but only to directly connected light clients that
   * asked for it via {@code want_response}. Peers and legacy clients never receive command 158 —
   * their read loops would desync on an unknown command byte.
   */
  private void respondToDeposit(
      Peer peer, FlaschenpostPut putMsg, im.redpanda.outbound.v1.Status status) {
    if (putMsg.getWantResponse() && peer.isLightClient() && outboundService != null) {
      outboundService.sendFlaschenpostPutResponse(peer, status);
    }
  }

  /**
   * Attempts to extract the destination KademliaId from a GarlicMessage-formatted payload and
   * deposit it into a locally registered Outbound Handle mailbox.
   *
   * <p><b>Scheduled for removal (MS02b domain-separation decision):</b> this legacy fallback treats
   * a 20-byte garlic <em>node</em> destination directly as an {@code oh_id}, so OH ids and node
   * KademliaIds share one undifferentiated namespace (a registered OH can shadow a node id). It
   * only exists because the explicit {@code oh_id} field was added after the first prototype; the
   * frontend has sent an explicit {@code oh_id} since Frontend-MS01. Once no legacy traffic
   * remains, remove this method and the implicit shared-namespace behavior — new code must never
   * rely on it.
   *
   * @return true if the deposit targeted a locally registered OH (stored or rejected by the MS02b
   *     hardening — in both cases the packet is handled here)
   */
  private boolean tryDepositToLocalOh(byte[] content) {
    if (outboundService == null) {
      return false;
    }
    // GarlicMessage format: [1 gmType][4 overallLen][20 destinationKademliaId]...
    int headerLen = 1 + 4 + KademliaId.ID_LENGTH_BYTES;
    if (content.length < headerLen) {
      return false;
    }
    try {
      byte[] ohId = new byte[KademliaId.ID_LENGTH_BYTES];
      System.arraycopy(content, 1 + 4, ohId, 0, KademliaId.ID_LENGTH_BYTES);
      // Anything other than NOT_FOUND targeted a locally registered OH: a rejected deposit
      // (quota/size) is handled here and must not leak into the legacy forwarding pipeline.
      return outboundService.depositMessage(ohId, content)
          != OutboundService.DepositResult.NOT_FOUND;
    } catch (RuntimeException e) {
      logger.warn("Failed to extract destination or deposit message to local OH", e);
      return false;
    }
  }
}
