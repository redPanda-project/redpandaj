package im.redpanda.updater;

import static im.redpanda.updater.UpdateTransfer.MAX_FUTURE_SKEW_MS;
import static im.redpanda.updater.UpdateTransfer.SIGNATURE_LEN;
import static im.redpanda.updater.UpdateTransfer.appendToWriteBuffer;
import static im.redpanda.updater.UpdateTransfer.reporting;
import static im.redpanda.updater.UpdateTransfer.requestUpdateContent;
import static im.redpanda.updater.UpdateTransfer.updateApkPath;

import im.redpanda.core.Command;
import im.redpanda.core.ConnectionReaderThread;
import im.redpanda.core.Log;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Android app distribution over the peer wire protocol: commands 13–16 ({@code
 * ANDROID_UPDATE_REQUEST_TIMESTAMP} … {@code ANDROID_UPDATE_ANSWER_CONTENT}) plus the verified
 * install of a received android.apk.
 *
 * <p>Moved verbatim out of {@code core.InboundCommandProcessor} by T116 (DDD review 2026-08-31, P2
 * step 3). Wire- and behaviour-invariant.
 *
 * <p>Note the asymmetry inherited from the original code: unlike the jar download, the apk download
 * serialises on {@link UpdateTransfer#updateUploadLock} (not the download lock). Preserved on
 * purpose — changing it would change the runtime behaviour of the deploy path.
 */
public class ApkUpdateHandler {

  private static final Logger logger = LogManager.getLogger();

  private final ServerContext serverContext;

  public ApkUpdateHandler(ServerContext serverContext) {
    this.serverContext = serverContext;
  }

  /** Command 13: a peer asks which apk version we have — answered only if we actually have one. */
  public int handleRequestTimestamp(Peer peer) {
    File file = updateApkPath().toFile();
    if (!file.exists()) {
      return 1;
    }
    long timestamp = serverContext.getLocalSettings().getUpdateAndroidTimestamp();
    peer.enqueueTimestamp(Command.ANDROID_UPDATE_ANSWER_TIMESTAMP, timestamp);
    return 1;
  }

  /** Command 14: a peer told us its apk version; download it if it is newer than ours. */
  public int handleAnswerTimestamp(ByteBuffer readBuffer, Peer peer) {
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
            UpdateTransfer.updateUploadLock.acquireUninterruptibly();
            try {
              if (othersTimestamp <= serverContext.getLocalSettings().getUpdateAndroidTimestamp()) {
                return;
              }
              System.out.println(
                  "our android.apk version is outdated, we try to download it from this peer!");
              if (!requestUpdateContent(peer, Command.ANDROID_UPDATE_REQUEST_CONTENT)) {
                return;
              }
              try {
                Thread.sleep(60000);
              } catch (InterruptedException ignored) {
              }
            } finally {
              System.out.println("we can now download it from another peer...");
              UpdateTransfer.updateUploadLock.release();
            }
          };
      ConnectionReaderThread.threadPool.submit(
          reporting("android-update-request-content-download", runnable));
    }
    return 1 + 8;
  }

  /**
   * Command 15: a peer asks for our signed apk; re-verify it and upload it off the reader thread.
   */
  public int handleRequestContent(Peer peer) {
    if (serverContext.getLocalSettings().getUpdateAndroidSignature() == null) {
      System.out.println(
          "we dont have an official signature to upload that android.apk update to other peers!");
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
              if (!appendToWriteBuffer(peer, a)) {
                return;
              }
              int cnt = 0;
              while (cnt < 6) {
                cnt++;
                try {
                  Thread.sleep(10000);
                } catch (InterruptedException ignored) {
                }
                // hasQueuedOutboundBytes() re-reads both buffers under writeBufferLock:
                // disconnect() nulls them while holding it.
                if (!peer.hasQueuedOutboundBytes()) {
                  break;
                }
                System.out.println("peer still downloading...");
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
          } finally {
            UpdateTransfer.updateUploadLock.release();
          }
        };
    ConnectionReaderThread.threadPool.submit(
        reporting("android-update-answer-content-upload", runnable));
    return 1;
  }

  /** Command 16: a peer sent us a signed apk; verify it and write it off the reader thread. */
  public int handleAnswerContent(ByteBuffer readBuffer, Peer peer) {
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

      // Writing the apk to disk and persisting settings is blocking disk I/O; offload it to the
      // thread pool so the ConnectionReaderThread is not stalled while it happens (REDPANDAJ-2DQ),
      // matching the request-side handlers. othersTimestamp/signature/data are already captured
      // above so nothing here races the reader moving on to the next command.
      ConnectionReaderThread.threadPool.submit(
          reporting(
              "install-apk-update", () -> installApkUpdate(othersTimestamp, signature, data)));
    }
    return consumedBytes;
  }

  /**
   * Writes a verified apk update to disk and persists the new timestamp/signature. Runs on {@link
   * ConnectionReaderThread#threadPool}, off the ConnectionReaderThread (REDPANDAJ-2DQ).
   */
  void installApkUpdate(long othersTimestamp, byte[] signature, byte[] data) {
    UpdateTransfer.installThreadHookForTests.accept(Thread.currentThread());
    try (FileOutputStream fos = new FileOutputStream(updateApkPath().toFile())) {
      fos.write(data);
    } catch (IOException e) {
      // Do not persist the new timestamp/signature if the apk was not actually written: that
      // would make LocalSettings claim an update is installed while the file is missing/corrupt.
      e.printStackTrace();
      return;
    }
    serverContext.getLocalSettings().setUpdateAndroidTimestamp(othersTimestamp);
    serverContext.getLocalSettings().setUpdateAndroidSignature(signature);
    serverContext.getLocalSettings().save(serverContext.getPort());
  }
}
