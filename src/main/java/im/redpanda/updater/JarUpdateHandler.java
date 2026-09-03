package im.redpanda.updater;

import static im.redpanda.updater.UpdateTransfer.MAX_FUTURE_SKEW_MS;
import static im.redpanda.updater.UpdateTransfer.SIGNATURE_LEN;
import static im.redpanda.updater.UpdateTransfer.appendToWriteBuffer;
import static im.redpanda.updater.UpdateTransfer.reporting;
import static im.redpanda.updater.UpdateTransfer.requestUpdateContent;
import static im.redpanda.updater.UpdateTransfer.updateInstallPath;
import static im.redpanda.updater.UpdateTransfer.updateInstallTmpPath;
import static im.redpanda.updater.UpdateTransfer.updateJarPath;

import im.redpanda.core.Command;
import im.redpanda.core.ConnectionReaderThread;
import im.redpanda.core.Log;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import im.redpanda.core.Settings;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Node software distribution: wire commands 9–12 ({@code UPDATE_REQUEST_TIMESTAMP} … {@code
 * UPDATE_ANSWER_CONTENT}) plus the verified install of a received redpanda.jar.
 *
 * <p>Moved verbatim out of {@code core.InboundCommandProcessor} by T116 (DDD review 2026-08-31, P2
 * step 3): the updater is its own bounded context (N-UPDATER) and only shares the command namespace
 * with the messaging protocol. Wire- and behaviour-invariant — same frames, same order, same
 * verification, same log lines. The next auto-updater deploy runs through this class.
 */
public class JarUpdateHandler {

  private static final Logger logger = LogManager.getLogger();

  private final ServerContext serverContext;

  public JarUpdateHandler(ServerContext serverContext) {
    this.serverContext = serverContext;
  }

  /** Command 9: a peer asks which jar version we have. */
  public int handleRequestTimestamp(Peer peer) {
    long timestamp = serverContext.getLocalSettings().getUpdateTimestamp();
    peer.enqueueTimestamp(Command.UPDATE_ANSWER_TIMESTAMP, timestamp);
    return 1;
  }

  /** Command 10: a peer told us its jar version; download it if it is newer than ours. */
  public int handleAnswerTimestamp(ByteBuffer readBuffer, Peer peer) {
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
            UpdateTransfer.updateDownloadLock.lock();
            try {
              System.out.println("our version is outdated, we try to download it from this peer!");
              if (!requestUpdateContent(peer, Command.UPDATE_REQUEST_CONTENT)) {
                return;
              }
              try {
                Thread.sleep(60000);
              } catch (InterruptedException ignored) {
              }
            } finally {
              System.out.println("we can now download it from another peer...");
              UpdateTransfer.updateDownloadLock.unlock();
            }
          };
      Server.threadPool.submit(reporting("update-request-content-download", runnable));
    }
    return 1 + 8;
  }

  /** Command 11: a peer asks for our signed jar; upload it off the reader thread. */
  public int handleRequestContent(Peer peer) {
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
          UpdateTransfer.updateUploadLock.acquireUninterruptibly();
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
              if (!appendToWriteBuffer(peer, a)) {
                return;
              }
            } catch (FileNotFoundException e) {
              Log.sentry(e);
              e.printStackTrace();
            } catch (IOException e) {
              // Copilot on #332: the moved code reported the same exception to Sentry twice
              // (a copy-paste in the original). Report once, and keep the stack trace like the
              // FileNotFoundException branch above.
              Log.sentry(e);
              e.printStackTrace();
            }
          } finally {
            UpdateTransfer.updateUploadLock.release();
          }
        };
    ConnectionReaderThread.threadPool.submit(reporting("update-answer-content-upload", runnable));
    return 1;
  }

  /** Command 12: a peer sent us a signed jar; verify it and install it off the reader thread. */
  public int handleAnswerContent(ByteBuffer readBuffer, Peer peer) {
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

      // Writing the (potentially tens-of-MB) jar to disk, moving it into place and persisting
      // settings are blocking disk I/O; offload it to the thread pool so the ConnectionReaderThread
      // is not stalled while it happens (REDPANDAJ-2DQ), matching the request-side handlers (see
      // handleRequestContent above). Everything the reader thread would otherwise need to read from
      // the connection buffer has already been captured above (othersTimestamp, signatureBytes,
      // data), so nothing here races the reader moving on to the next command.
      ConnectionReaderThread.threadPool.submit(
          reporting(
              "install-jar-update", () -> installJarUpdate(othersTimestamp, signatureBytes, data)));
    }
    return consumedBytes;
  }

  /**
   * Writes a verified jar update to disk, installs it and triggers the restart. Runs on {@link
   * ConnectionReaderThread#threadPool}, off the ConnectionReaderThread (REDPANDAJ-2DQ) — keep the
   * write, move, settings save and restart trigger together and in this order so the process never
   * restarts (or persists a timestamp/signature) before the jar is actually in place.
   */
  void installJarUpdate(long othersTimestamp, byte[] signatureBytes, byte[] data) {
    UpdateTransfer.installThreadHookForTests.accept(Thread.currentThread());
    // Resolve the install path exactly once and derive the tmp path from it, so both stay
    // consistent even if the overriding system property changes while the install is running.
    Path installPath = updateInstallPath();
    Path tmpPath = updateInstallTmpPath(installPath);
    try (FileOutputStream fos = new FileOutputStream(tmpPath.toFile())) {
      fos.write(data);
    } catch (IOException e) {
      Log.sentry(e);
      return;
    }

    try {
      // Install the update
      // Save to 'update' file so the shell script can pick it up and restart
      Files.move(tmpPath, installPath, StandardCopyOption.REPLACE_EXISTING);

      // Update local settings
      serverContext.getLocalSettings().setUpdateTimestamp(othersTimestamp);
      serverContext.getLocalSettings().setUpdateSignature(signatureBytes);
      serverContext.getLocalSettings().save(serverContext.getPort());

      System.out.println(
          "Update successfully verified and saved to '"
              + installPath
              + "'. New timestamp: "
              + othersTimestamp);
      System.out.println("Stopping server to apply update...");

      // Exit asynchronously to allow current method to return and log to be written
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  Thread.sleep(2000);
                } catch (InterruptedException e) {
                  // Preserve the interrupt status and skip the restart instead of silently
                  // continuing as if the delay had completed normally (e.g. on shutdown).
                  Thread.currentThread().interrupt();
                  return;
                }
                UpdateTransfer.restartAction.run();
              });

    } catch (IOException e) {
      Log.sentry(e);
      System.out.println("Failed to install update: " + e.getMessage());
    }
  }
}
