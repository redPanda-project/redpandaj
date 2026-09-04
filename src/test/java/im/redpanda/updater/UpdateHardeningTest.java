package im.redpanda.updater;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import im.redpanda.core.Command;
import im.redpanda.core.LocalSettings;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import im.redpanda.ops.Settings;
import im.redpanda.transport.ByteBufferPool;
import im.redpanda.transport.InboundCommandProcessor;
import im.redpanda.transport.Peer;
import im.redpanda.transport.PeerTestSupport;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the T10 (updater option B) hardening added to the UPDATE_ANSWER_CONTENT handler: rollback
 * protection via a build-time floor, a future-timestamp skew guard, and fail-closed behaviour when
 * no updater key is configured. See PLAN-updater-option-b.md, PR 1.
 *
 * <p>All update file paths are redirected into a per-test {@link TempDir} via the {@code
 * redpanda.update.*.path} system properties (the PR #246 pattern): Surefire runs 8 forks sharing
 * one working directory, and other classes (InboundCommandProcessorAsyncUpdatesTest,
 * InboundCommandProcessorMoreCoverageTest) delete CWD-relative {@code tmp_redpanda.jar} in their
 * cleanup, which could race this class's write/move window in another fork (T70 flakiness).
 */
class UpdateHardeningTest {

  private static final int TEST_PORT = 49781;

  private static final String INSTALL_PATH_PROPERTY = "redpanda.update.install.path";
  private static final String APK_PATH_PROPERTY = "redpanda.update.apk.path";
  private static final String JAR_PATH_PROPERTY = "redpanda.update.jar.path";

  @TempDir File tempDir;

  /** Redirected install destination (CWD-relative {@code update} in production). */
  private File updateFile;

  /** Redirected staging file, always a sibling of {@link #updateFile}. */
  private File tmpJarFile;

  /** Redirected apk destination ({@link UpdateTransfer#ANDROID_UPDATE_FILE} in prod). */
  private File apkFile;

  /**
   * Staging file the apk install writes before moving it onto {@link #apkFile} (T121/TD127).
   * Derived from the production rule rather than spelled out, so the name scheme lives in exactly
   * one place — {@link #apkStagingPathIsASiblingNamedAfterItsDestination} pins that rule.
   */
  private File apkTmpFile;

  /**
   * Redirected "the jar this node runs and serves" ({@code redpanda.jar} in the CWD in prod). Left
   * non-existent by default, which is exactly the pre-T117 situation for the update floor (mtime
   * 0), so the tests that predate it are unaffected.
   */
  private File runningJarFile;

  private ServerContext ctx;
  private InboundCommandProcessor proc;

  /** The real pool, put back in {@link #cleanup()} after a test substituted a direct one. */
  private Executor originalTaskPool;

  @BeforeEach
  void setup() {
    updateFile = new File(tempDir, "update");
    tmpJarFile = new File(tempDir, "tmp_redpanda.jar");
    apkFile = new File(tempDir, "android.apk");
    apkTmpFile = UpdateTransfer.updateApkTmpPath(apkFile.toPath()).toFile();
    runningJarFile = new File(tempDir, "redpanda.jar");
    System.setProperty(INSTALL_PATH_PROPERTY, updateFile.getAbsolutePath());
    System.setProperty(APK_PATH_PROPERTY, apkFile.getAbsolutePath());
    System.setProperty(JAR_PATH_PROPERTY, runningJarFile.getAbsolutePath());

    ctx = ServerContext.buildDefaultServerContext();
    ctx.setPort(TEST_PORT);
    proc = new InboundCommandProcessor(ctx);
    ByteBufferPool.init();
    Settings.seedNode = false;
    Settings.loadUpdates = false;
    // A download task holds the static UpdateTransfer.updateDownloadLock for this long after
    // asking a peer for content. Nothing in this class asserts anything about that window, and at
    // the production minute a task outliving its test wedges every later test in the fork, so the
    // hold is simply switched off for the whole class. NOT restored per test - see cleanup().
    UpdateTransfer.downloadHoldMillis = 0L;
    originalTaskPool = UpdateTransfer.updateTaskPool;
  }

  @AfterEach
  void cleanup() {
    System.clearProperty(INSTALL_PATH_PROPERTY);
    System.clearProperty(APK_PATH_PROPERTY);
    System.clearProperty(JAR_PATH_PROPERTY);
    Updater.resetPublicUpdaterKeyForTests();
    // Deliberately a no-op, NOT the production default System.exit(0): the positive-path tests
    // wait for their delayed restart trigger, but if such a test fails/times out before the
    // 2s-delayed background thread fires, restoring System.exit(0) here would let that straggler
    // thread kill the whole Surefire fork mid-suite.
    UpdateTransfer.restartAction = () -> {};
    UpdateTransfer.installThreadHookForTests = t -> {};
    UpdateTransfer.updateTaskPool = originalTaskPool;
    // downloadHoldMillis is deliberately NOT restored here. It used to be, and that was the
    // T121e flake: a download task submitted by the finishing test can reach its Thread.sleep
    // after @AfterEach has run, read the restored 60 s and hold the static updateDownloadLock for
    // a minute - long enough for the next test's 30 s wait to expire. The production value is put
    // back once, after the whole class (@AfterAll), when no task of ours can still be running.
    LocalSettings.settingsFile(TEST_PORT).delete();
  }

  @AfterAll
  static void restoreProductionDownloadHold() {
    UpdateTransfer.downloadHoldMillis = 60_000L;
  }

  /** A syntactically valid (fixed 64-byte Ed25519) but cryptographically fake signature. */
  private static byte[] fakeSignature() {
    byte[] sig = new byte[NodeId.SIGNATURE_LEN];
    for (int i = 0; i < sig.length; i++) sig[i] = (byte) i;
    return sig;
  }

  private static ByteBuffer buildUpdateAnswerContent(
      long timestamp, byte[] signature, byte[] data) {
    ByteBuffer in = ByteBuffer.allocate(8 + 4 + signature.length + data.length);
    in.putLong(timestamp);
    in.putInt(data.length);
    in.put(signature);
    in.put(data);
    in.flip();
    return in;
  }

  /**
   * Runs every update task on the calling thread instead of the pool, so a task the handler queues
   * has already finished when {@code parseCommand} returns.
   *
   * <p>This is what makes the assertions below plain assertions. Polling a buffer for up to 30 s
   * "until the pool got round to it" is not just slow, it is unfalsifiable in the negative
   * direction and, as T121e showed, it fails for reasons that have nothing to do with the code
   * under test. Restored in {@link #cleanup()}. Not for the tests that assert a task runs OFF the
   * calling thread ({@code validUpdate_offloadsDiskWriteToThreadPool}), which need the real pool.
   */
  private void runUpdateTasksOnTheCallingThread() {
    UpdateTransfer.updateTaskPool = UpdateTransfer.SAME_THREAD_TASK_POOL;
  }

  private Peer newPeer(int port) {
    Peer peer = new Peer("127.0.0.1", port, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);
    return peer;
  }

  @Test
  void downgrade_rejected_whenTimestampNotAboveLocal() {
    long localTs = Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L;
    ctx.getLocalSettings().setUpdateTimestamp(localTs);

    byte[] data = new byte[] {1, 2, 3};
    byte[] sig = fakeSignature();
    // othersTimestamp == localTs -> not strictly greater -> rejected as a downgrade/replay.
    ByteBuffer in = buildUpdateAnswerContent(localTs, sig, data);

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, newPeer(8801));

    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
    assertFalse(tmpJarFile.exists(), "tmp file must not be written");
    assertFalse(updateFile.exists(), "update file must not be written");
    assertEquals(localTs, ctx.getLocalSettings().getUpdateTimestamp());
  }

  @Test
  void downgrade_rejected_belowBuildFloor_onFreshLocalSettings() {
    // Fresh LocalSettings: updateTimestamp == -1 (see LocalSettings() ctor).
    assertEquals(-1L, ctx.getLocalSettings().getUpdateTimestamp());

    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    byte[] data = new byte[] {5, 6, 7, 8};
    // Validly signed, but not above the compile-time floor -> still rejected.
    long othersTs = Updater.MIN_UPDATE_TIMESTAMP_MS;
    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(othersTs);
    toHash.put(data);
    byte[] sig = testKey.sign(toHash.array());

    ByteBuffer in = buildUpdateAnswerContent(othersTs, sig, data);

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, newPeer(8802));

    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
    assertFalse(tmpJarFile.exists(), "tmp file must not be written");
    assertFalse(updateFile.exists(), "update file must not be written");
    assertEquals(-1L, ctx.getLocalSettings().getUpdateTimestamp());
  }

  @Test
  void futureSkew_rejected() {
    long othersTs = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(25);
    byte[] data = new byte[] {1, 1, 1};
    byte[] sig = fakeSignature();

    ByteBuffer in = buildUpdateAnswerContent(othersTs, sig, data);

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, newPeer(8803));

    assertEquals(
        1 + 8 + 4 + sig.length + data.length,
        consumed,
        "handler must report exactly the consumed byte length or the parser desyncs");
    assertFalse(tmpJarFile.exists(), "tmp file must not be written");
    assertFalse(updateFile.exists(), "update file must not be written");
  }

  @Test
  void placeholderKey_failClosed_noFileWritten() {
    // Force the fail-closed state via the test override instead of relying on the (now real,
    // non-placeholder) PUBLIC_SIGNING_KEY_OF_CORE_DEVELOPERS constant.
    Updater.setPublicUpdaterKeyForTests(null);

    byte[] data = new byte[] {2, 4, 6, 8};
    long othersTs = Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L;
    byte[] sig = fakeSignature();

    ByteBuffer in = buildUpdateAnswerContent(othersTs, sig, data);

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, newPeer(8804));

    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
    assertFalse(tmpJarFile.exists(), "tmp file must not be written");
    assertFalse(updateFile.exists(), "update file must not be written");
    assertEquals(-1L, ctx.getLocalSettings().getUpdateTimestamp());
  }

  @Test
  void validUpdate_installs_andInvokesRestartAction() throws Exception {
    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    // The restart trigger is the last step of the install (fired 2s after the jar is moved into
    // place and the settings are saved), so it is the one deterministic "install finished" signal:
    // once the latch opens, all other side effects are visible and can be asserted directly
    // instead of being polled for.
    AtomicInteger restartCount = new AtomicInteger();
    CountDownLatch restartLatch = new CountDownLatch(1);
    UpdateTransfer.restartAction =
        () -> {
          restartCount.incrementAndGet();
          restartLatch.countDown();
        };

    byte[] data = "fake-jar-bytes".getBytes();
    long othersTs = Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L;
    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(othersTs);
    toHash.put(data);
    byte[] sig = testKey.sign(toHash.array());

    ByteBuffer in = buildUpdateAnswerContent(othersTs, sig, data);

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, newPeer(8805));
    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);

    // Waiting for the restart also guarantees the 2s-delayed background thread does not outlive
    // this test method (in the success path) and fire into a later test.
    assertTrue(restartLatch.await(10, TimeUnit.SECONDS), "restart action not invoked within 10 s");
    assertEquals(1, restartCount.get());

    assertTrue(updateFile.exists(), "update file must exist after the install completed");
    assertTrue(
        java.util.Arrays.equals(data, Files.readAllBytes(updateFile.toPath())),
        "installed update file must contain the received data");
    assertEquals(othersTs, ctx.getLocalSettings().getUpdateTimestamp());
    assertTrue(java.util.Arrays.equals(sig, ctx.getLocalSettings().getUpdateSignature()));
  }

  @Test
  void validUpdate_offloadsDiskWriteToThreadPool() throws Exception {
    // Regression test for REDPANDAJ-2DQ: handleUpdateAnswerContent used to do its
    // FileOutputStream/Files.move/LocalSettings.save work synchronously on the calling thread
    // (the ConnectionReaderThread in production), which could stall the reader for as long as the
    // disk write takes. Assert the write happens off the calling thread.
    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    // Deterministic "install finished" signal, same as validUpdate_installs_andInvokesRestartAction
    // (see comment there): await the restart latch, then assert all side effects directly.
    AtomicInteger restartCount = new AtomicInteger();
    CountDownLatch restartLatch = new CountDownLatch(1);
    UpdateTransfer.restartAction =
        () -> {
          restartCount.incrementAndGet();
          restartLatch.countDown();
        };

    AtomicReference<Thread> writerThread = new AtomicReference<>();
    UpdateTransfer.installThreadHookForTests = writerThread::set;

    byte[] data = "fake-jar-bytes-for-offload-check".getBytes();
    long othersTs = Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L;
    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(othersTs);
    toHash.put(data);
    byte[] sig = testKey.sign(toHash.array());

    ByteBuffer in = buildUpdateAnswerContent(othersTs, sig, data);

    Thread callingThread = Thread.currentThread();
    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, newPeer(8814));
    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);

    assertTrue(restartLatch.await(10, TimeUnit.SECONDS), "restart action not invoked within 10 s");
    assertEquals(1, restartCount.get());

    assertTrue(updateFile.exists(), "update file must exist after the install completed");
    assertNotNull(writerThread.get(), "install thread hook must have fired before the restart");
    assertNotEquals(
        callingThread,
        writerThread.get(),
        "the jar write/install must not run on the calling (reader) thread");
  }

  @Test
  void androidValidUpdate_installsAsynchronously() throws Exception {
    // Android analog of validUpdate_installs_andInvokesRestartAction: no positive-path test
    // existed for handleAndroidUpdateAnswerContent before REDPANDAJ-2DQ moved its disk write off
    // the ConnectionReaderThread; assert the eventual side effects (apk file + settings) land.
    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    byte[] data = "fake-apk-bytes".getBytes();
    long othersTs = Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L;
    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(othersTs);
    toHash.put(data);
    byte[] sig = testKey.sign(toHash.array());

    ByteBuffer in = buildUpdateAnswerContent(othersTs, sig, data);

    int consumed = proc.parseCommand(Command.ANDROID_UPDATE_ANSWER_CONTENT, in, newPeer(8815));
    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);

    // There is no restart trigger on the android path, so poll for the last observable install
    // step instead: installApkUpdate writes the apk, then sets the timestamp, then the signature
    // (Copilot review: waiting on the timestamp alone could pass before the signature is set).
    // Once the signature matches, timestamp and apk file are stable and can be asserted directly.
    awaitCondition(
        () -> java.util.Arrays.equals(sig, ctx.getLocalSettings().getUpdateAndroidSignature()),
        10_000);
    assertEquals(othersTs, ctx.getLocalSettings().getUpdateAndroidTimestamp());
    assertTrue(apkFile.exists(), "apk file must exist after the install completed");
    assertTrue(
        java.util.Arrays.equals(data, Files.readAllBytes(apkFile.toPath())),
        "installed apk file must contain the received data");
    // T121/TD127: written to a sibling tmp file and moved, so the staging file is consumed by the
    // move rather than left lying around next to the apk we serve.
    assertFalse(apkTmpFile.exists(), "the apk staging file must not survive a successful install");
  }

  @Test
  void negativeContentLength_disconnectsPeer() {
    ByteBuffer in = ByteBuffer.allocate(8 + 4 + NodeId.SIGNATURE_LEN);
    in.putLong(Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L);
    in.putInt(-1); // network-controlled length: protocol violation
    in.put(fakeSignature());
    in.flip();

    Peer peer = newPeer(8806);
    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, peer);

    assertEquals(0, consumed);
    assertFalse(peer.isConnected(), "peer must be disconnected on negative content length");
    assertFalse(tmpJarFile.exists(), "tmp file must not be written");
  }

  @Test
  void androidNegativeContentLength_disconnectsPeer() {
    ByteBuffer in = ByteBuffer.allocate(8 + 4 + NodeId.SIGNATURE_LEN);
    in.putLong(Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L);
    in.putInt(Integer.MIN_VALUE);
    in.put(fakeSignature());
    in.flip();

    Peer peer = newPeer(8807);
    int consumed = proc.parseCommand(Command.ANDROID_UPDATE_ANSWER_CONTENT, in, peer);

    assertEquals(0, consumed);
    assertFalse(peer.isConnected(), "peer must be disconnected on negative android content length");
    assertFalse(apkFile.exists(), "apk file must not be written");
  }

  @Test
  void androidFutureSkew_rejected() {
    long othersTs = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(25);
    byte[] data = new byte[] {3, 3, 3};
    byte[] sig = fakeSignature();

    ByteBuffer in = buildUpdateAnswerContent(othersTs, sig, data);

    int consumed = proc.parseCommand(Command.ANDROID_UPDATE_ANSWER_CONTENT, in, newPeer(8808));

    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
    assertFalse(apkFile.exists(), "apk file must not be written");
  }

  @Test
  void androidDowngrade_rejected_belowBuildFloor_onFreshLocalSettings() {
    byte[] data = new byte[] {4, 4, 4, 4};
    byte[] sig = fakeSignature();
    // Not above the compile-time floor -> rejected before any verification.
    ByteBuffer in = buildUpdateAnswerContent(Updater.MIN_UPDATE_TIMESTAMP_MS, sig, data);

    int consumed = proc.parseCommand(Command.ANDROID_UPDATE_ANSWER_CONTENT, in, newPeer(8809));

    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
    assertFalse(apkFile.exists(), "apk file must not be written");
    assertEquals(0L, ctx.getLocalSettings().getUpdateAndroidTimestamp());
  }

  @Test
  void updateAnswerTimestamp_futureSkew_rejected() {
    Settings.loadUpdates = true;
    ByteBuffer in = ByteBuffer.allocate(8);
    in.putLong(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(25));
    in.flip();

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_TIMESTAMP, in, newPeer(8810));

    assertEquals(1 + 8, consumed);
  }

  @Test
  void updateAnswerTimestamp_belowBuildFloor_noDownload() {
    Settings.loadUpdates = true;
    // Above the fresh local timestamp (-1) but not above the build floor -> no download.
    ByteBuffer in = ByteBuffer.allocate(8);
    in.putLong(Updater.MIN_UPDATE_TIMESTAMP_MS);
    in.flip();

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_TIMESTAMP, in, newPeer(8811));

    assertEquals(1 + 8, consumed);
  }

  @Test
  void androidUpdateAnswerTimestamp_futureSkew_rejected() {
    ByteBuffer in = ByteBuffer.allocate(8);
    in.putLong(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(25));
    in.flip();

    int consumed = proc.parseCommand(Command.ANDROID_UPDATE_ANSWER_TIMESTAMP, in, newPeer(8812));

    assertEquals(1 + 8, consumed);
  }

  @Test
  void androidUpdateAnswerTimestamp_belowBuildFloor_noDownload() {
    ByteBuffer in = ByteBuffer.allocate(8);
    in.putLong(Updater.MIN_UPDATE_TIMESTAMP_MS);
    in.flip();

    int consumed = proc.parseCommand(Command.ANDROID_UPDATE_ANSWER_TIMESTAMP, in, newPeer(8813));

    assertEquals(1 + 8, consumed);
  }

  // --- T121: apk install and lock hygiene (TD127, TD125) ---

  /**
   * TD191. Both timestamp answers (commands 9 and 13) discarded the {@code enqueueTimestamp} return
   * value, so a peer that disconnected between its request and our answer was dropped in complete
   * silence — indistinguishable, from the requesting side, from a node that refuses to answer,
   * which is exactly the shape of the "peer has outdated version" confusion of deploy #7. The drop
   * itself is correct (the peer asks again after reconnecting); it just has to be visible. What
   * must not change is the consumed byte count, or the parser desyncs.
   */
  @Test
  void timestampAnswers_toAGonePeer_areDroppedWithoutDesyncingTheParser() throws Exception {
    Files.write(apkFile.toPath(), new byte[] {1, 2, 3}); // else command 13 answers nothing at all
    // Go through the real disconnect path rather than just never giving the peer a buffer:
    // disconnect() nulls writeBuffer under writeBufferLock, and that is the state
    // enqueueTimestamp reports false for.
    Peer gone = newPeer(8823);
    PeerTestSupport.initWriteBuffer(gone, 64);
    gone.disconnect("test: gone before we could answer");
    assertFalse(gone.enqueueCommand(Command.PING), "precondition: the peer must be unwritable");

    assertEquals(
        1, proc.parseCommand(Command.UPDATE_REQUEST_TIMESTAMP, ByteBuffer.allocate(0), gone));
    assertEquals(
        1,
        proc.parseCommand(Command.ANDROID_UPDATE_REQUEST_TIMESTAMP, ByteBuffer.allocate(0), gone));
  }

  /**
   * The publish step must survive a filesystem without an atomic rename instead of leaving the
   * update unstaged: the fallback replacing move is still strictly better than writing the
   * destination directly, which is what TD127 was about.
   */
  @Test
  void publishStagedFile_fallsBackWhenTheMoveCannotBeAtomic() throws Exception {
    java.nio.file.Path staging = new File(tempDir, "staged").toPath();
    java.nio.file.Path destination = new File(tempDir, "published").toPath();
    Files.write(staging, "new".getBytes());
    Files.write(destination, "old".getBytes());

    UpdateTransfer.publishStagedFile(staging, destination);

    assertTrue(
        java.util.Arrays.equals("new".getBytes(), Files.readAllBytes(destination)),
        "the staged bytes must be the ones that end up published");
    assertFalse(Files.exists(staging), "the staging file is consumed by the publish");
  }

  /**
   * The one place the staging-name scheme is asserted (every other test derives its expectation
   * from {@link UpdateTransfer#updateApkTmpPath}). Sibling, so the {@code Files.move} stays within
   * one filesystem and is atomic; named after the destination, so the 8 Surefire forks — each with
   * its own {@code target/android-N.apk} — do not stage through one shared file, which is the
   * collision that made the update tests flaky before T70.
   */
  @Test
  void apkStagingPathIsASiblingNamedAfterItsDestination() {
    java.nio.file.Path apk = java.nio.file.Path.of("target", "android-3.apk");
    java.nio.file.Path staging = UpdateTransfer.updateApkTmpPath(apk);

    assertEquals(apk.getParent(), staging.getParent(), "must be a sibling of the destination");
    assertNotEquals(apk.getFileName(), staging.getFileName(), "must not be the destination itself");
    assertTrue(
        staging.getFileName().toString().contains(apk.getFileName().toString()),
        "must carry the destination name so per-fork paths stay distinct: " + staging);
  }

  /** Builds a correctly signed ANDROID_UPDATE_ANSWER_CONTENT frame for {@code key}. */
  private static ByteBuffer signedApkContent(NodeId key, long timestamp, byte[] data) {
    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(timestamp);
    toHash.put(data);
    return buildUpdateAnswerContent(timestamp, key.sign(toHash.array()), data);
  }

  /**
   * TD127. {@code installApkUpdate} wrote the received bytes straight into {@code android.apk},
   * which truncated the apk we serve the moment the stream opened. Anything that stopped the write
   * from finishing — a crash, a full disk, a short write — left a truncated file that {@code
   * handleRequestContent} and {@code HTTPServer} then handed out, while the old apk was already
   * gone. Since T121 the bytes go to a sibling staging file first and only an atomic move publishes
   * them.
   *
   * <p>The failure is injected by putting a <em>directory</em> where the staging file belongs, so
   * the {@code FileOutputStream} fails deterministically on open, with no timing involved. Against
   * the pre-T121 code this test is red: there the same open targets the destination itself and
   * succeeds, replacing the apk.
   */
  @Test
  void apkInstall_failedStagingWrite_leavesTheServedApkIntact() throws Exception {
    byte[] oldApk = "the-apk-we-currently-serve".getBytes();
    Files.write(apkFile.toPath(), oldApk);
    long installedAt = Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(30);
    assertTrue(apkFile.setLastModified(installedAt), "could not set the apk mtime");
    assertTrue(apkTmpFile.mkdir(), "could not block the staging path with a directory");

    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    AtomicInteger installsRun = new AtomicInteger();
    UpdateTransfer.installThreadHookForTests = t -> installsRun.incrementAndGet();
    runUpdateTasksOnTheCallingThread();

    byte[] data = "the-apk-that-cannot-be-staged".getBytes();
    long othersTs = installedAt + TimeUnit.HOURS.toMillis(1);
    int consumed =
        proc.parseCommand(
            Command.ANDROID_UPDATE_ANSWER_CONTENT,
            signedApkContent(testKey, othersTs, data),
            newPeer(8818));
    assertEquals(1 + 8 + 4 + NodeId.SIGNATURE_LEN + data.length, consumed);

    // The install ran inline, so there is nothing left in flight to wait for: everything it could
    // have done it has already done.
    assertEquals(1, installsRun.get(), "the install task must have run");
    assertTrue(
        java.util.Arrays.equals(oldApk, Files.readAllBytes(apkFile.toPath())),
        "a failed apk install must not touch the apk we serve");
    assertEquals(
        0L,
        ctx.getLocalSettings().getUpdateAndroidTimestamp(),
        "a failed apk install must not claim the update is installed");
  }

  /**
   * TD125, half one. The apk download used to acquire {@code UpdateTransfer.updateUploadLock}, so
   * an upload in progress (a peer pulling our jar or apk, up to a minute) stopped us from even
   * asking for a newer apk — and, the other way round, our apk download blocked every upload.
   * Downloads and uploads are independent; only downloads serialise against each other.
   */
  @Test
  void apkDownload_isNotHeldUpByAnUpload() {
    Peer peer = newPeer(8819);
    ByteBuffer out = PeerTestSupport.initWriteBuffer(peer, 4096);
    runUpdateTasksOnTheCallingThread();

    UpdateTransfer.updateUploadLock.acquireUninterruptibly(); // an upload is in flight
    try {
      // On a separate thread only so a regression (the download task taking the upload lock this
      // thread holds) shows up as a failed join with a message, instead of deadlocking the fork.
      // The task itself still runs inline on that thread, so the join returns the moment the
      // handler is done - nothing here waits on a clock.
      runToCompletion(
          () ->
              proc.parseCommand(
                  Command.ANDROID_UPDATE_ANSWER_TIMESTAMP,
                  offer(Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(30)),
                  peer),
          "the download task must not wait for the upload lock");
    } finally {
      UpdateTransfer.updateUploadLock.release();
    }

    out.flip();
    assertEquals(
        Command.ANDROID_UPDATE_REQUEST_CONTENT,
        out.get(),
        "an upload in flight must not stop us from requesting a newer apk");
  }

  /**
   * TD125, half two. The lock the download does take is the download lock, so it queues behind
   * another download instead of running next to it — "at most one update download at a time" was
   * never true before, whatever the field name said.
   *
   * <p>Drives {@link UpdateTransfer#downloadTask} directly (T121e): the handler's only job here is
   * to hand it a command byte and a floor, and going through the handler and the pool added two
   * schedulings that the assertions then had to poll for. The lock hand-over is a latch handshake,
   * so nothing waits on a clock in the passing case.
   */
  @Test
  void downloadTask_waitsForARunningDownload() throws Exception {
    Peer peer = newPeer(8820);
    ByteBuffer out = PeerTestSupport.initWriteBuffer(peer, 4096);
    long offered = Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(30);

    CountDownLatch holderHasTheLock = new CountDownLatch(1);
    CountDownLatch releaseTheLock = new CountDownLatch(1);
    AtomicReference<Throwable> holderFailure = new AtomicReference<>();
    AtomicReference<Throwable> downloadFailure = new AtomicReference<>();
    Thread holder =
        startRecording(
            () -> {
              UpdateTransfer.updateDownloadLock.lock();
              try {
                holderHasTheLock.countDown();
                releaseTheLock.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                UpdateTransfer.updateDownloadLock.unlock();
              }
            },
            holderFailure);
    assertTrue(
        holderHasTheLock.await(10, TimeUnit.SECONDS), "the stand-in download never took the lock");

    Thread download =
        startRecording(
            UpdateTransfer.downloadTask(
                "android.apk",
                peer,
                Command.ANDROID_UPDATE_REQUEST_CONTENT,
                offered,
                () -> Updater.MIN_UPDATE_TIMESTAMP_MS),
            downloadFailure);

    // "nothing happened" needs a window - but a window can only fail if something IS queued, never
    // because the machine was slow, which is the difference to the wait this test used to do.
    assertStaysEmpty(
        out, TimeUnit.SECONDS.toMillis(1), "a download must queue behind a running download");

    releaseTheLock.countDown();
    joinOrFail(holder, "the stand-in download never released the lock");
    joinOrFail(download, "the deferred download never ran after the lock was released");
    assertNothingThrown(holderFailure, "the stand-in download");
    assertNothingThrown(downloadFailure, "the deferred download");

    out.flip();
    assertEquals(
        Command.ANDROID_UPDATE_REQUEST_CONTENT,
        out.get(),
        "the request must be deferred, not dropped");
  }

  /**
   * TD125/#347 follow-up: the re-check the download task does after waiting for the lock reads the
   * floor again, so a download that whoever held the lock before us has already made pointless is
   * dropped rather than requested.
   *
   * <p>The floor is the task's parameter, so "the floor moved while we waited" is expressed by what
   * the supplier answers — no lock hand-over, no thread, no window (T121e). The second half is the
   * control that keeps the first from passing for the wrong reason.
   */
  @Test
  void downloadTask_isDroppedWhenTheFloorMovedWhileWaiting() {
    Peer peer = newPeer(8821);
    ByteBuffer out = PeerTestSupport.initWriteBuffer(peer, 4096);
    long offered = Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(30);

    UpdateTransfer.downloadTask(
            "android.apk",
            peer,
            Command.ANDROID_UPDATE_REQUEST_CONTENT,
            offered,
            () -> offered) // the floor caught up with the offer while we waited
        .run();
    assertEquals(0, out.position(), "we must not request an apk that is no longer newer than us");

    UpdateTransfer.downloadTask(
            "android.apk", peer, Command.ANDROID_UPDATE_REQUEST_CONTENT, offered, () -> offered - 1)
        .run();
    out.flip();
    assertEquals(
        Command.ANDROID_UPDATE_REQUEST_CONTENT,
        out.get(),
        "with the floor still behind the offer the very same task does request the apk");
  }

  /**
   * The handler half of the above: what it hands the task as the floor must be {@code
   * androidUpdateFloor()}, i.e. include the stored apk's mtime, not just the recorded timestamp
   * (which is still 0 on fresh settings here).
   */
  @Test
  void apkOffer_belowTheStoredApkMtime_neverReachesTheDownloadTask() throws Exception {
    Peer peer = newPeer(8824);
    ByteBuffer out = PeerTestSupport.initWriteBuffer(peer, 4096);
    runUpdateTasksOnTheCallingThread();

    long installedAt = Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(30);
    Files.write(apkFile.toPath(), new byte[] {1, 2, 3});
    assertTrue(apkFile.setLastModified(installedAt), "could not set the apk mtime");
    assertEquals(0L, ctx.getLocalSettings().getUpdateAndroidTimestamp(), "fresh settings");

    proc.parseCommand(
        Command.ANDROID_UPDATE_ANSWER_TIMESTAMP,
        offer(installedAt - TimeUnit.HOURS.toMillis(1)),
        peer);

    assertEquals(0, out.position(), "an apk older than the one we hold must not be requested");
  }

  /**
   * Runs {@code body} on its own thread and waits for it to finish.
   *
   * <p>The wait is a {@link Thread#join(long)} on work that has no timer in it, not a poll for a
   * condition that may or may not become true: it returns the instant the body is done. The
   * deadline exists so a regression that makes the body block on a lock fails with {@code message}
   * instead of wedging the Surefire fork.
   */
  private static void runToCompletion(Runnable body, String message) {
    AtomicReference<Throwable> failure = new AtomicReference<>();
    joinOrFail(startRecording(body, failure), message);
    assertNothingThrown(failure, message);
  }

  /**
   * Starts {@code body} on a virtual thread, recording anything it throws into {@code failure}.
   *
   * <p>Without this a body that blows up takes its exception to the grave: the thread dies, the
   * join returns, {@code isAlive()} is false, and the test passes having asserted nothing.
   */
  private static Thread startRecording(Runnable body, AtomicReference<Throwable> failure) {
    return Thread.ofVirtual()
        .start(
            () -> {
              try {
                body.run();
              } catch (Throwable t) {
                failure.set(t);
              }
            });
  }

  /** Fails with the recorded cause if the thread {@link #startRecording} started threw. */
  private static void assertNothingThrown(AtomicReference<Throwable> failure, String message) {
    Throwable thrown = failure.get();
    if (thrown != null) {
      throw new AssertionError(message + ": the work threw on its own thread", thrown);
    }
  }

  /**
   * Waits for a thread whose work contains no timer; the deadline only turns a hang into a fail.
   */
  private static void joinOrFail(Thread worker, String message) {
    try {
      worker.join(java.time.Duration.ofSeconds(10));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail("interrupted while waiting for: " + message);
    }
    if (worker.isAlive()) {
      fail(message + " (still running after 10 s)");
    }
  }

  private static void awaitCondition(BooleanSupplier condition, long timeoutMillis) {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (System.nanoTime() < deadlineNanos) {
      if (condition.getAsBoolean()) {
        return;
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
    }
    fail("Condition not met within " + timeoutMillis + "ms");
  }

  // --- T117 follow-up: the update floor of a node whose settings were regenerated ---

  /**
   * Writes the file that stands in for the jar this node is running, with {@code lastModified} set
   * to {@code installedAt} — the moment the running jar was installed here.
   */
  private void installedJarWithMtime(long installedAt) throws Exception {
    Files.write(runningJarFile.toPath(), new byte[] {9, 9, 9});
    assertTrue(runningJarFile.setLastModified(installedAt), "could not set the jar mtime");
  }

  /**
   * The regression of the T117 deploy on 2026-09-03. The new storage format deliberately does not
   * read the pre-T117 settings file, so both Hetzner nodes came up with {@code updateTimestamp ==
   * -1}; the floor collapsed to the 2026-07-11 build constant and they accepted, within a second of
   * starting, a correctly signed but 75 minutes OLDER jar from a peer that still ran the previous
   * release — and downgraded themselves.
   *
   * <p>With no recorded timestamp the mtime of the jar we are running is the floor, so an update
   * that predates our own installation is a rollback and is refused.
   */
  @Test
  void downgrade_rejected_onFreshSettings_whenOlderThanTheInstalledJar() throws Exception {
    assertEquals(-1L, ctx.getLocalSettings().getUpdateTimestamp(), "fresh settings");
    long installedAt = Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(30);
    installedJarWithMtime(installedAt);

    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    byte[] data = new byte[] {5, 6, 7, 8};
    // correctly signed and well above the build-time constant, but older than our own jar
    long othersTs = installedAt - TimeUnit.HOURS.toMillis(1);
    assertTrue(othersTs > Updater.MIN_UPDATE_TIMESTAMP_MS, "this must not pass on the old floor");
    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(othersTs);
    toHash.put(data);
    byte[] sig = testKey.sign(toHash.array());

    // The install runs on a thread pool, so "nothing happened" needs a window, not an instant
    // assertion - without that window this test stays green even with the floor removed.
    CountDownLatch restarted = new CountDownLatch(1);
    UpdateTransfer.restartAction = restarted::countDown;

    int consumed =
        proc.parseCommand(
            Command.UPDATE_ANSWER_CONTENT,
            buildUpdateAnswerContent(othersTs, sig, data),
            newPeer(8811));

    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
    assertFalse(
        restarted.await(5, TimeUnit.SECONDS), "a rollback must not trigger the restart/install");
    assertFalse(updateFile.exists(), "a rollback must not be installed");
    assertEquals(-1L, ctx.getLocalSettings().getUpdateTimestamp());
  }

  /**
   * TD160, the noise half of the T117 identity-reset story. A node whose {@code LocalSettings} were
   * regenerated has {@code updateTimestamp == -1}, so before #343/#347 it re-accepted the update it
   * was already running: same sha, same signature, one extra download and a second restart per
   * deploy. It is not a downgrade, which is why it was filed as noise — but it is a restart nobody
   * asked for, in the middle of a deploy.
   *
   * <p>The unconditional floor of #347 closes it without a dedup check of its own, and this is the
   * exact boundary case: an offer whose timestamp <em>equals</em> the mtime of the jar we run. That
   * is what a re-offer of our own release looks like when the deploy preserved the mtime; a
   * re-offer through the updater itself is strictly older than the mtime (the file was written when
   * it was installed, after it was signed) and is covered by {@link
   * #downgrade_rejected_onFreshSettings_whenOlderThanTheInstalledJar}. Either way the floor is
   * {@code >=} the offer and {@code >} is required, so nothing happens.
   *
   * <p>Known residual, deliberately not fixed here: if {@code UpdateTransfer.updateJarPath()} names
   * a file that does not exist — a checkout run out of an IDE, a client layout without the jar —
   * the mtime term is 0 and the floor falls back to {@link Updater#MIN_UPDATE_TIMESTAMP_MS}, where
   * the extra cycle is still possible. Closing that would mean deriving the floor from the running
   * artefact (e.g. the code source location), which on such a layout is a class directory whose
   * mtime is the last compile — that would push the floor to "now" and block real updates. The
   * layout that matters (a node started from {@code redpanda.jar}, which is also the only one where
   * {@code Settings.init} enables updates at all) always has the file.
   */
  @Test
  void sameReleaseAfterIdentityReset_isNotInstalledAgain() throws Exception {
    assertEquals(-1L, ctx.getLocalSettings().getUpdateTimestamp(), "settings just regenerated");
    long installedAt = Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(30);
    installedJarWithMtime(installedAt);

    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    // A peer re-offers the very release we are running: correctly signed, well above the build
    // constant, and with no recorded timestamp to compare against.
    byte[] data = new byte[] {9, 9, 9};
    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(installedAt);
    toHash.put(data);
    byte[] sig = testKey.sign(toHash.array());

    CountDownLatch restarted = new CountDownLatch(1);
    UpdateTransfer.restartAction = restarted::countDown;

    int consumed =
        proc.parseCommand(
            Command.UPDATE_ANSWER_CONTENT,
            buildUpdateAnswerContent(installedAt, sig, data),
            newPeer(8822));

    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
    assertFalse(
        restarted.await(5, TimeUnit.SECONDS),
        "re-installing the release we are already running is the extra restart of TD160");
    assertFalse(updateFile.exists(), "nothing may be installed");
    assertEquals(-1L, ctx.getLocalSettings().getUpdateTimestamp());
  }

  /** The other side of the guard: a genuinely newer update is still installed on fresh settings. */
  @Test
  void update_accepted_onFreshSettings_whenNewerThanTheInstalledJar() throws Exception {
    long installedAt = Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(30);
    installedJarWithMtime(installedAt);

    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    byte[] data = new byte[] {1, 2, 3, 4};
    long othersTs = installedAt + TimeUnit.HOURS.toMillis(1);
    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(othersTs);
    toHash.put(data);
    byte[] sig = testKey.sign(toHash.array());

    CountDownLatch restarted = new CountDownLatch(1);
    UpdateTransfer.restartAction = restarted::countDown;

    proc.parseCommand(
        Command.UPDATE_ANSWER_CONTENT,
        buildUpdateAnswerContent(othersTs, sig, data),
        newPeer(8812));

    assertTrue(restarted.await(30, TimeUnit.SECONDS), "the update must be installed");
    assertTrue(updateFile.exists());
    assertEquals(othersTs, ctx.getLocalSettings().getUpdateTimestamp());
  }

  /**
   * The floor must also stop us from even asking for an older jar (command 10) — in both shapes:
   * with no recorded timestamp at all (T117c) and with a recorded timestamp that lags behind the
   * jar we run (T117d).
   *
   * <p>One test rather than two on purpose: the positive control at the end submits the real
   * download runnable, which holds the static {@code UpdateTransfer.updateDownloadLock} for 60 s. A
   * second test doing the same would block on that lock instead of measuring anything.
   */
  @Test
  void olderOffer_neverRequestsContent_withOrWithoutARecordedTimestamp() throws Exception {
    long installedAt = Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(30);
    installedJarWithMtime(installedAt);
    Settings.loadUpdates = true;
    // Every task the handler queues runs before parseCommand returns, so "nothing was queued" is
    // a fact the moment the call comes back rather than something to watch for a while. That is
    // what removes the 30 s positive wait this test used to end with (T121e: it expired on a
    // loaded CI fork and turned a correct implementation red).
    runUpdateTasksOnTheCallingThread();
    try {
      Peer peer = newPeer(8813);
      ByteBuffer out = PeerTestSupport.initWriteBuffer(peer, 4096);

      // (a) fresh settings: the record is -1, only the jar can carry the floor
      assertEquals(-1L, ctx.getLocalSettings().getUpdateTimestamp());
      proc.parseCommand(
          Command.UPDATE_ANSWER_TIMESTAMP, offer(installedAt - TimeUnit.HOURS.toMillis(1)), peer);
      assertEquals(0, out.position(), "we must not ask a peer for an older jar");

      // (b) T117d: a recorded timestamp from the PREVIOUS deploy, behind the jar we run. This is
      // what deploy #7 hit - the offer passes the record but is still a rollback.
      ctx.getLocalSettings().setUpdateTimestamp(installedAt - TimeUnit.HOURS.toMillis(2));
      proc.parseCommand(
          Command.UPDATE_ANSWER_TIMESTAMP, offer(installedAt - TimeUnit.HOURS.toMillis(1)), peer);
      assertEquals(0, out.position(), "we must not ask a peer for an older jar");

      // Positive control: an offer above the floor does reach requestUpdateContent through this
      // very harness, so both assertions above are not vacuously true.
      proc.parseCommand(
          Command.UPDATE_ANSWER_TIMESTAMP, offer(installedAt + TimeUnit.HOURS.toMillis(1)), peer);
      out.flip();
      assertEquals(
          Command.UPDATE_REQUEST_CONTENT, out.get(), "an offer above the floor is asked for");
    } finally {
      Settings.loadUpdates = false;
    }
  }

  /** An UPDATE_ANSWER_TIMESTAMP payload announcing {@code timestamp}. */
  private static ByteBuffer offer(long timestamp) {
    ByteBuffer in = ByteBuffer.allocate(8);
    in.putLong(timestamp);
    in.flip();
    return in;
  }

  /**
   * Fails as soon as anything is queued on {@code buffer}, polling for {@code windowMillis}. A
   * single check after a fixed sleep would pass whenever the queueing is merely late (CI load).
   */
  private static void assertStaysEmpty(ByteBuffer buffer, long windowMillis, String message) {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(windowMillis);
    while (System.nanoTime() < deadlineNanos) {
      assertEquals(0, buffer.position(), message);
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
    }
    assertEquals(0, buffer.position(), message);
  }

  /**
   * Befund 2 of the 2026-09-03 deploy: peers running the previous release kept logging "peer has
   * outdated redPandaj version" about the updated nodes. What a node answers to command 9 must be
   * exactly what its settings file says — not the in-memory default of a LocalSettings that was
   * constructed before the file was read.
   */
  @Test
  void timestampAnswer_isTheValueFromTheSettingsFile() {
    long persisted = Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(45);
    LocalSettings written = LocalSettings.load(TEST_PORT);
    written.setUpdateTimestamp(persisted);
    written.setUpdateSignature(fakeSignature());
    written.save(TEST_PORT);

    ctx.setLocalSettings(LocalSettings.load(TEST_PORT));
    assertEquals(persisted, ctx.getLocalSettings().getUpdateTimestamp(), "loaded from the file");

    Peer peer = newPeer(8814);
    ByteBuffer out = PeerTestSupport.initWriteBuffer(peer, 64);

    proc.parseCommand(Command.UPDATE_REQUEST_TIMESTAMP, ByteBuffer.allocate(0), peer);

    out.flip();
    assertEquals(Command.UPDATE_ANSWER_TIMESTAMP, out.get(), "command byte");
    assertEquals(persisted, out.getLong(), "the answered timestamp must be the persisted one");
  }

  // --- T117d: a RECORDED timestamp behind the running jar must not re-open the hole ---

  /**
   * Deploy #7 on 2026-09-03. node1 came up on the new build but still carried {@code
   * updateTimestamp = 1788466702516} from the previous deploy, i.e. its settings were NOT fresh —
   * so the T117c floor used that stale record and ignored the jar. It then accepted {@code
   * 1788471100532}: older than the jar it was actually running, but newer than the record.
   * Downgrade, third restart, and a good outcome only because the uploader was still pushing.
   *
   * <p>Since T117d the jar mtime is always part of the floor, so an offer between the stale record
   * and our own installation is refused.
   */
  @Test
  void downgrade_rejected_whenRecordedTimestampIsBehindTheInstalledJar() throws Exception {
    long installedAt = Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(30);
    installedJarWithMtime(installedAt);
    // the settings are NOT fresh - they hold the timestamp of the deploy before this one
    long staleRecord = installedAt - TimeUnit.HOURS.toMillis(2);
    ctx.getLocalSettings().setUpdateTimestamp(staleRecord);

    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    byte[] data = new byte[] {7, 7, 7};
    // strictly between the stale record and our own jar: the T117c floor let exactly this through
    long othersTs = installedAt - TimeUnit.HOURS.toMillis(1);
    assertTrue(othersTs > staleRecord, "must pass the recorded-timestamp check");
    assertTrue(othersTs < installedAt, "must be older than the jar we run");
    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(othersTs);
    toHash.put(data);
    byte[] sig = testKey.sign(toHash.array());

    CountDownLatch restarted = new CountDownLatch(1);
    UpdateTransfer.restartAction = restarted::countDown;

    proc.parseCommand(
        Command.UPDATE_ANSWER_CONTENT,
        buildUpdateAnswerContent(othersTs, sig, data),
        newPeer(8816));

    assertFalse(
        restarted.await(5, TimeUnit.SECONDS), "a rollback must not trigger the restart/install");
    assertFalse(updateFile.exists(), "a rollback must not be installed");
    assertEquals(staleRecord, ctx.getLocalSettings().getUpdateTimestamp());
  }

  /** The other side: with the same stale record, a jar newer than ours is still installed. */
  @Test
  void update_accepted_whenNewerThanTheInstalledJar_despiteAStaleRecord() throws Exception {
    long installedAt = Updater.MIN_UPDATE_TIMESTAMP_MS + TimeUnit.DAYS.toMillis(30);
    installedJarWithMtime(installedAt);
    ctx.getLocalSettings().setUpdateTimestamp(installedAt - TimeUnit.HOURS.toMillis(2));

    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    byte[] data = new byte[] {8, 8, 8};
    long othersTs = installedAt + TimeUnit.HOURS.toMillis(1);
    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(othersTs);
    toHash.put(data);
    byte[] sig = testKey.sign(toHash.array());

    CountDownLatch restarted = new CountDownLatch(1);
    UpdateTransfer.restartAction = restarted::countDown;

    proc.parseCommand(
        Command.UPDATE_ANSWER_CONTENT,
        buildUpdateAnswerContent(othersTs, sig, data),
        newPeer(8817));

    assertTrue(restarted.await(30, TimeUnit.SECONDS), "the update must be installed");
    assertTrue(updateFile.exists());
    assertEquals(othersTs, ctx.getLocalSettings().getUpdateTimestamp());
  }
}
