package im.redpanda.updater;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import im.redpanda.core.ByteBufferPool;
import im.redpanda.core.Command;
import im.redpanda.core.InboundCommandProcessor;
import im.redpanda.core.LocalSettings;
import im.redpanda.core.NodeId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.core.Settings;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
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

  @TempDir File tempDir;

  /** Redirected install destination (CWD-relative {@code update} in production). */
  private File updateFile;

  /** Redirected staging file, always a sibling of {@link #updateFile}. */
  private File tmpJarFile;

  /** Redirected apk destination ({@link UpdateTransfer#ANDROID_UPDATE_FILE} in prod). */
  private File apkFile;

  private ServerContext ctx;
  private InboundCommandProcessor proc;

  @BeforeEach
  void setup() {
    updateFile = new File(tempDir, "update");
    tmpJarFile = new File(tempDir, "tmp_redpanda.jar");
    apkFile = new File(tempDir, "android.apk");
    System.setProperty(INSTALL_PATH_PROPERTY, updateFile.getAbsolutePath());
    System.setProperty(APK_PATH_PROPERTY, apkFile.getAbsolutePath());

    ctx = ServerContext.buildDefaultServerContext();
    ctx.setPort(TEST_PORT);
    proc = new InboundCommandProcessor(ctx);
    ByteBufferPool.init();
    Settings.seedNode = false;
    Settings.loadUpdates = false;
  }

  @AfterEach
  void cleanup() {
    System.clearProperty(INSTALL_PATH_PROPERTY);
    System.clearProperty(APK_PATH_PROPERTY);
    Updater.resetPublicUpdaterKeyForTests();
    // Deliberately a no-op, NOT the production default System.exit(0): the positive-path tests
    // wait for their delayed restart trigger, but if such a test fails/times out before the
    // 2s-delayed background thread fires, restoring System.exit(0) here would let that straggler
    // thread kill the whole Surefire fork mid-suite.
    UpdateTransfer.restartAction = () -> {};
    UpdateTransfer.installThreadHookForTests = t -> {};
    LocalSettings.settingsFile(TEST_PORT).delete();
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
}
