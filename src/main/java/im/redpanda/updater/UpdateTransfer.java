package im.redpanda.updater;

import im.redpanda.identity.NodeId;
import im.redpanda.ops.Log;
import im.redpanda.ops.Settings;
import im.redpanda.transport.ConnectionReaderThread;
import im.redpanda.transport.Peer;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Plumbing shared by the two software-distribution flows (jar and android apk).
 *
 * <p>Extracted verbatim from {@code core.InboundCommandProcessor} by the DDD review's P2 step 3
 * (T116): software distribution is its own bounded context (N-UPDATER) that merely happens to speak
 * commands 9–16 of the peer wire protocol. Nothing here changed behaviourally — same locks, same
 * paths, same log lines, same order.
 */
public final class UpdateTransfer {

  private static final Logger logger = LogManager.getLogger();

  private UpdateTransfer() {}

  /** Ed25519 signatures are fixed-size (64 bytes, no DER framing). */
  public static final int SIGNATURE_LEN = NodeId.SIGNATURE_LEN;

  /** Reject update timestamps further in the future than this (clock-skew / spoofing guard). */
  static final long MAX_FUTURE_SKEW_MS = 24L * 60 * 60 * 1000;

  /**
   * Local file a received android update is written to and that gets uploaded to peers requesting
   * it. Lived on {@code ConnectionReaderThread} before T116, which never used it.
   */
  public static final String ANDROID_UPDATE_FILE =
      System.getProperty("redpanda.android.update.file", "android.apk");

  /**
   * At most one update upload at a time (the upload sleeps and then pushes multiple megabytes into
   * a peer's write buffer). Lived on {@code ConnectionReaderThread} before T116.
   */
  static final Semaphore updateUploadLock = new Semaphore(1);

  /**
   * At most one update download at a time, jar and apk alike. Lived on {@code
   * ConnectionReaderThread} before T116.
   *
   * <p>T121/TD125: the apk download used to serialise on {@link #updateUploadLock} instead — a
   * copy-paste that made every incoming apk offer block all uploads for up to {@link
   * #downloadHoldMillis}, while doing nothing to stop a jar and an apk download from running at
   * once. Both downloads now take this lock, so "one download at a time" actually holds and an
   * upload is never held up by a download.
   */
  static final ReentrantLock updateDownloadLock = new ReentrantLock();

  /**
   * The one executor every update-distribution task runs on — both directions, both artefacts.
   *
   * <p>T121/TD126: the jar download went to {@code Server.threadPool} while the other five tasks
   * went to {@code ConnectionReaderThread.threadPool}. Both are unbounded {@code
   * newVirtualThreadPerTaskExecutor()}s, so the split never caused a stall, but it meant the pool a
   * task lands on depended on which handler queued it, and neither shutdown path covered all six.
   * {@code ConnectionReaderThread.threadPool} wins because these tasks exist to get the blocking
   * disk/socket work <em>off</em> the ConnectionReaderThread that queued them (REDPANDAJ-2DQ), and
   * because it was already carrying five of the six. Routing every submit through this one field is
   * what keeps them from drifting apart again.
   */
  static final ExecutorService updateTaskPool = ConnectionReaderThread.threadPool;

  /**
   * How long a download task keeps {@link #updateDownloadLock} after asking a peer for content —
   * the window in which that peer may deliver before we try another one.
   *
   * <p>Not a constant so tests can shorten it: a test that exercises the download path otherwise
   * leaves the static lock held for a full minute, which the next test in the same Surefire fork
   * then blocks on. Production never writes it.
   */
  static long downloadHoldMillis = 60_000L;

  /**
   * Invoked to apply an installed update. Default restarts the JVM; tests replace this with a
   * counter so the positive-path test never actually exits the test JVM.
   */
  static Runnable restartAction = () -> System.exit(0);

  /**
   * Test-only hook invoked with the thread that performs the update install disk I/O ({@link
   * JarUpdateHandler#installJarUpdate} / {@link ApkUpdateHandler#installApkUpdate}); lets tests
   * assert the write actually runs off the calling (ConnectionReaderThread, in production) thread
   * (REDPANDAJ-2DQ). No-op in production.
   */
  static java.util.function.Consumer<Thread> installThreadHookForTests = t -> {};

  /**
   * Wraps an update-distribution task so unchecked failures are reported instead of vanishing.
   *
   * <p>These tasks go to {@code ExecutorService.submit()} and nobody ever looks at the returned
   * {@code Future}, so any {@code RuntimeException} was swallowed without a log line or a Sentry
   * event. The upload runnables dereference {@code peer.writeBuffer} / {@code
   * peer.writeBufferCrypted} after sleeping up to 60 s, and {@code Peer.disconnect(String)} nulls
   * exactly those fields — a peer disconnecting inside that window produced a silent NPE.
   *
   * <p>This wrapper only makes such a failure visible; it does not make the task bodies safe. Each
   * body is responsible for its own cleanup, and two of them were not: the update-answer-timestamp
   * handlers did {@code lock(); put(); unlock();} with no {@code finally}, so the NPE left {@code
   * writeBufferLock} held forever. That is fixed at the source — see {@link
   * #requestUpdateContent(Peer, byte)} and {@link #appendToWriteBuffer(Peer, ByteBuffer)}, which
   * hold the lock in a {@code try/finally} and abort cleanly when the peer is gone.
   *
   * <p>{@link Error}s are reported and rethrown; only unchecked exceptions are absorbed.
   */
  public static Runnable reporting(String taskName, Runnable task) {
    return () -> {
      try {
        task.run();
      } catch (RuntimeException e) {
        logger.warn("update task '{}' failed", taskName, e);
        Log.sentry(e);
      } catch (Error e) {
        logger.error("update task '{}' failed fatally", taskName, e);
        Log.sentry(e);
        throw e;
      }
    };
  }

  /**
   * Writes a single update-request command byte into the peer's write buffer.
   *
   * <p>{@code Peer.disconnect(String)} nulls {@code writeBuffer} while holding {@code
   * writeBufferLock}, so the field is re-read and checked under that lock — which is what {@link
   * Peer#enqueueCommand(byte)} does (T115: the locking used to be spelled out at this call site,
   * and the version before that did {@code lock(); put(); unlock();} with no {@code finally}, so a
   * disconnect in that window did not only NPE, it left the lock permanently held).
   *
   * @return {@code true} if the command was queued, {@code false} if the peer is gone
   */
  public static boolean requestUpdateContent(Peer peer, byte command) {
    if (!peer.enqueueCommand(command)) {
      logger.info("peer disconnected before the update could be requested, aborting");
      return false;
    }
    return true;
  }

  /**
   * Appends a fully built update frame to the peer's write buffer, growing the buffer if needed.
   *
   * <p>Same re-read-under-the-lock contract as {@link #requestUpdateContent(Peer, byte)}: the frame
   * is built after a {@code Thread.sleep} and a multi-megabyte disk read, so the peer may well be
   * gone by the time we get here. Dereferencing the nulled {@code writeBuffer} raised an NPE inside
   * a {@code Runnable} whose {@code Future} nobody observes — no log, no Sentry. The growth policy
   * itself is {@link Peer#enqueueGrowingFrame(ByteBuffer)}'s (T115).
   *
   * @return {@code true} if the frame was queued, {@code false} if the peer is gone
   */
  public static boolean appendToWriteBuffer(Peer peer, ByteBuffer frame) {
    if (!peer.enqueueGrowingFrame(frame)) {
      logger.info("peer disconnected before the update could be uploaded, aborting");
      return false;
    }
    return true;
  }

  /** System property overriding {@link #updateJarPath()}; used by tests to avoid CWD sharing. */
  static final String JAR_PATH_PROPERTY = "redpanda.update.jar.path";

  /** System property overriding {@link #updateApkPath()}; used by tests to avoid CWD sharing. */
  static final String APK_PATH_PROPERTY = "redpanda.update.apk.path";

  /**
   * System property overriding {@link #updateInstallPath()}; used by tests to avoid CWD sharing.
   */
  static final String INSTALL_PATH_PROPERTY = "redpanda.update.install.path";

  /**
   * Path of the local redpanda.jar that gets uploaded to peers requesting it. Defaults to the usual
   * seed-node vs. client layout, overridable via {@value #JAR_PATH_PROPERTY} (tests only, so
   * Surefire forks sharing the working directory don't collide).
   */
  static Path updateJarPath() {
    Path override = pathOverride(JAR_PATH_PROPERTY);
    if (override != null) {
      return override;
    }
    return Settings.isSeedNode() ? Path.of("target/redpanda.jar") : Path.of("redpanda.jar");
  }

  /**
   * Path of the local android.apk that gets uploaded to peers requesting it / that a received
   * update is written to. Defaults to {@link #ANDROID_UPDATE_FILE}, overridable via {@value
   * #APK_PATH_PROPERTY} (tests only).
   */
  static Path updateApkPath() {
    Path override = pathOverride(APK_PATH_PROPERTY);
    if (override != null) {
      return override;
    }
    return Path.of(ANDROID_UPDATE_FILE);
  }

  /**
   * Destination a received-and-verified jar update is installed to (the {@code update} file the
   * restart shell script picks up). Defaults to the CWD-relative {@code update} file, overridable
   * via {@value #INSTALL_PATH_PROPERTY} (tests only, so Surefire forks sharing the working
   * directory don't collide).
   */
  static Path updateInstallPath() {
    Path override = pathOverride(INSTALL_PATH_PROPERTY);
    if (override != null) {
      return override;
    }
    return Path.of("update");
  }

  /**
   * Staging file a jar update is written to before being moved to the given install destination.
   * Derived as a sibling of that destination so the {@code Files.move} never crosses a filesystem
   * boundary (and so a test override relocates both files together). Takes the already-resolved
   * install path instead of re-reading the system property, so a property change mid-install (e.g.
   * a test cleaning up after a timeout) cannot make the tmp file and the move destination diverge.
   */
  static Path updateInstallTmpPath(Path installPath) {
    return installPath.resolveSibling("tmp_redpanda.jar");
  }

  /**
   * Staging file a received apk is written to before being moved onto the given destination. Same
   * contract as {@link #updateInstallTmpPath(Path)}: a sibling of the destination, so the {@code
   * Files.move} stays within one filesystem and is therefore atomic, and derived from the
   * already-resolved destination rather than from the system property again.
   *
   * <p>The destination file name is part of the staging name (unlike the jar's fixed {@code
   * tmp_redpanda.jar}) because the apk path is <em>not</em> redirected per test: Surefire hands
   * each of the 8 forks its own {@code target/android-N.apk} via {@code
   * redpanda.android.update.file}, and a shared {@code target/tmp_android.apk} would put the forks
   * back on one file — exactly the collision that made the update tests flaky before T70.
   */
  static Path updateApkTmpPath(Path apkPath) {
    return apkPath.resolveSibling("tmp_" + apkPath.getFileName());
  }

  /**
   * Reads a path-override system property, ignoring it (falling back to the caller's default) when
   * it is blank or not a valid path, so a misconfigured test property can't crash the update
   * handlers with an unchecked {@link java.nio.file.InvalidPathException}.
   */
  private static Path pathOverride(String property) {
    String value = System.getProperty(property);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Path.of(value);
    } catch (java.nio.file.InvalidPathException e) {
      logger.warn("ignoring invalid {} override: {}", property, value);
      return null;
    }
  }
}
