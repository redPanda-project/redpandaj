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
import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import im.redpanda.ops.Log;
import im.redpanda.ops.Settings;
import im.redpanda.transport.Peer;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
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
    if (!peer.enqueueTimestamp(Command.UPDATE_ANSWER_TIMESTAMP, timestamp)) {
      // The peer disconnected between sending the request and us answering it. Nothing to repair
      // — it will ask again after reconnecting — but the drop used to be entirely silent, which
      // during a deploy looks exactly like a node that refuses to answer.
      logger.debug(
          "could not queue UPDATE_ANSWER_TIMESTAMP for {}: peer already disconnected", peer);
    }
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
    long floor = updateFloor();
    if (othersTimestamp < serverContext.getLocalSettings().getUpdateTimestamp()) {
      // debug: during a rollout this fires for every timestamp exchange with every node that has
      // not been updated yet, i.e. for half the network for as long as the rollout takes.
      logger.debug(
          "peer {} has an outdated redPandaj version: {} < ours {}",
          peer.getNodeId(),
          othersTimestamp,
          serverContext.getLocalSettings().getUpdateTimestamp());
    }
    if (othersTimestamp > floor && Settings.isLoadUpdates()) {
      Runnable runnable =
          () -> {
            UpdateTransfer.updateDownloadLock.lock();
            try {
              // info: this is the first line of an update actually happening on this node, and
              // the anchor for the deploy watch.
              logger.info(
                  "our jar is outdated, requesting the {} build from {}",
                  othersTimestamp,
                  peer.getNodeId());
              if (!requestUpdateContent(peer, Command.UPDATE_REQUEST_CONTENT)) {
                return;
              }
              try {
                Thread.sleep(UpdateTransfer.downloadHoldMillis);
              } catch (InterruptedException ignored) {
              }
            } finally {
              logger.debug("jar download slot released, another peer may serve us now");
              UpdateTransfer.updateDownloadLock.unlock();
            }
          };
      // TD126: one pool for every update task, see UpdateTransfer.updateTaskPool. This was the
      // single submit that went to Server.threadPool instead.
      UpdateTransfer.updateTaskPool.submit(reporting("update-request-content-download", runnable));
    }
    return 1 + 8;
  }

  /**
   * The oldest update timestamp this node still accepts: the highest of what we recorded, when the
   * jar we are running was put here, and the build-time constant. Both the offer (command 10) and
   * the delivery (command 12) check against it, so an unsolicited push is rejected just like a
   * pull.
   *
   * <p><b>Why the jar mtime is always part of it (T117d).</b> T117c used it only when there was no
   * recorded timestamp, which left the mirror-image hole open: a node whose <i>recorded</i>
   * timestamp is behind the jar it actually runs still accepted the older jar. Deploy #7 on
   * 2026-09-03 walked straight into it — node1 came up on the new build carrying {@code
   * updateTimestamp = 1788466702516} from the previous deploy, accepted {@code 1788471100532} (a
   * jar older than the one it was running, but newer than that stale record), downgraded itself,
   * and only recovered because the uploader was still pushing. Three restarts instead of two. The
   * same shape occurs after a rollback deploy, a deploy race, or a jar copied in by hand.
   *
   * <p>The mtime is sound as a floor because a signature timestamp is the build time of that jar
   * and a jar cannot have been installed here before it was built: {@code signature timestamp <=
   * install time}. So every update older than our own installation is a rollback, whatever the
   * settings happen to say.
   *
   * <p>{@link Updater#MIN_UPDATE_TIMESTAMP_MS} stays in the expression on purpose (TD168). It is
   * the only term that survives a missing jar file (mtime {@code 0} — a client layout, a test) on
   * fresh settings, and the only one that is immune to a wrong system clock. It costs nothing and
   * it is the documented knob of the release runbook.
   *
   * <p>Deliberate trade-off, unchanged from T117c and now slightly wider: a node that installs a
   * jar long after it was signed refuses updates signed before that installation. That costs one
   * more signed release — and every deploy signs a fresh build, so a real release is always newer
   * than any node's install time. The case it prevents silently downgrades the network.
   */
  private long updateFloor() {
    return Math.max(
        Math.max(serverContext.getLocalSettings().getUpdateTimestamp(), installedJarTimestamp()),
        Updater.MIN_UPDATE_TIMESTAMP_MS);
  }

  private static long installedJarTimestamp() {
    return updateJarPath().toFile().lastModified();
  }

  /** Command 11: a peer asks for our signed jar; upload it off the reader thread. */
  public int handleRequestContent(Peer peer) {
    if (serverContext.getLocalSettings().getUpdateTimestamp() == -1) {
      return 1;
    }
    if (serverContext.getLocalSettings().getUpdateSignature() == null) {
      logger.debug("no official signature for our jar, not uploading it to {}", peer.getNodeId());
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
              // info: the uploading half of a deploy; the pair of this line and the receiver's
              // "installed" line is what a deploy watch follows.
              logger.info(
                  "sending our jar ({}) to {}",
                  serverContext.getLocalSettings().getUpdateTimestamp(),
                  peer.getNodeId());
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
              logger.warn("the jar we are supposed to upload is not at {}", path, e);
              Log.sentry(e);
            } catch (IOException e) {
              // Copilot on #332: the moved code reported the same exception to Sentry twice
              // (a copy-paste in the original). Report once, with the stack trace on the log
              // record rather than on stderr (T121d).
              logger.warn("could not read our jar at {} to upload it", path, e);
              Log.sentry(e);
            }
          } finally {
            UpdateTransfer.updateUploadLock.release();
          }
        };
    UpdateTransfer.updateTaskPool.submit(reporting("update-answer-content-upload", runnable));
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
    long floor = updateFloor();
    if (othersTimestamp > floor) {

      // Verify signature before writing anything
      NodeId publicUpdaterKey = Updater.getPublicUpdaterKey();
      if (publicUpdaterKey == null) {
        logger.warn("no public updater key available, cannot verify the jar update from {}", peer);
        return consumedBytes;
      }

      ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
      toHash.putLong(othersTimestamp);
      toHash.put(data);

      if (!publicUpdaterKey.verify(toHash.array(), signatureBytes)) {
        logger.warn("jar update from {} rejected: signature invalid", peer);
        return consumedBytes;
      }

      // Writing the (potentially tens-of-MB) jar to disk, moving it into place and persisting
      // settings are blocking disk I/O; offload it to the thread pool so the ConnectionReaderThread
      // is not stalled while it happens (REDPANDAJ-2DQ), matching the request-side handlers (see
      // handleRequestContent above). Everything the reader thread would otherwise need to read from
      // the connection buffer has already been captured above (othersTimestamp, signatureBytes,
      // data), so nothing here races the reader moving on to the next command.
      UpdateTransfer.updateTaskPool.submit(
          reporting(
              "install-jar-update", () -> installJarUpdate(othersTimestamp, signatureBytes, data)));
    }
    return consumedBytes;
  }

  /**
   * Writes a verified jar update to disk, installs it and triggers the restart. Runs on {@link
   * UpdateTransfer#updateTaskPool}, off the ConnectionReaderThread (REDPANDAJ-2DQ) — keep the
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
      UpdateTransfer.publishStagedFile(tmpPath, installPath);

      // Update local settings
      serverContext.getLocalSettings().setUpdateTimestamp(othersTimestamp);
      serverContext.getLocalSettings().setUpdateSignature(signatureBytes);
      serverContext.getLocalSettings().save(serverContext.getPort());

      logger.info(
          "jar update verified and installed to '{}', new timestamp {}; stopping the server to"
              + " apply it",
          installPath,
          othersTimestamp);

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
      logger.error("failed to install the jar update to '{}'", installPath, e);
    }
  }
}
