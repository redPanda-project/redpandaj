package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers the T10 (updater option B) hardening added to the UPDATE_ANSWER_CONTENT handler: rollback
 * protection via a build-time floor, a future-timestamp skew guard, and fail-closed behaviour when
 * no updater key is configured. See PLAN-updater-option-b.md, PR 1.
 */
class InboundCommandProcessorUpdateHardeningTest {

  private static final int TEST_PORT = 49781;

  private ServerContext ctx;
  private InboundCommandProcessor proc;

  @BeforeEach
  void setup() {
    ctx = ServerContext.buildDefaultServerContext();
    ctx.setPort(TEST_PORT);
    proc = new InboundCommandProcessor(ctx);
    ByteBufferPool.init();
    Settings.seedNode = false;
    Settings.loadUpdates = false;
  }

  @AfterEach
  void cleanup() {
    Updater.resetPublicUpdaterKeyForTests();
    InboundCommandProcessor.restartAction = () -> System.exit(0);
    InboundCommandProcessor.installThreadHookForTests = t -> {};
    new File("tmp_redpanda.jar").delete();
    new File("update").delete();
    new File(ConnectionReaderThread.ANDROID_UPDATE_FILE).delete();
    new File(Settings.SAVE_DIR + "/localSettings" + TEST_PORT + ".dat").delete();
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
    assertFalse(new File("tmp_redpanda.jar").exists(), "tmp file must not be written");
    assertFalse(new File("update").exists(), "update file must not be written");
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
    assertFalse(new File("tmp_redpanda.jar").exists(), "tmp file must not be written");
    assertFalse(new File("update").exists(), "update file must not be written");
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
    assertFalse(new File("tmp_redpanda.jar").exists(), "tmp file must not be written");
    assertFalse(new File("update").exists(), "update file must not be written");
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
    assertFalse(new File("tmp_redpanda.jar").exists(), "tmp file must not be written");
    assertFalse(new File("update").exists(), "update file must not be written");
    assertEquals(-1L, ctx.getLocalSettings().getUpdateTimestamp());
  }

  @Test
  void validUpdate_installs_andInvokesRestartAction() throws Exception {
    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    AtomicInteger restartCount = new AtomicInteger();
    InboundCommandProcessor.restartAction = restartCount::incrementAndGet;

    byte[] data = "fake-jar-bytes".getBytes();
    long othersTs = Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L;
    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(othersTs);
    toHash.put(data);
    byte[] sig = testKey.sign(toHash.array());

    ByteBuffer in = buildUpdateAnswerContent(othersTs, sig, data);

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, newPeer(8805));
    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);

    File updateFile = new File("update");
    awaitCondition(updateFile::exists, 5000);
    assertTrue(
        java.util.Arrays.equals(data, Files.readAllBytes(updateFile.toPath())),
        "installed update file must contain the received data");

    assertEquals(othersTs, ctx.getLocalSettings().getUpdateTimestamp());
    assertTrue(java.util.Arrays.equals(sig, ctx.getLocalSettings().getUpdateSignature()));

    // The restart happens 2s after installing, asynchronously; wait for it and make sure the
    // JVM (this test process) is still alive to observe it.
    awaitCondition(() -> restartCount.get() == 1, 5000);
    assertEquals(1, restartCount.get());
  }

  @Test
  void validUpdate_offloadsDiskWriteToThreadPool() throws Exception {
    // Regression test for REDPANDAJ-2DQ: handleUpdateAnswerContent used to do its
    // FileOutputStream/Files.move/LocalSettings.save work synchronously on the calling thread
    // (the ConnectionReaderThread in production), which could stall the reader for as long as the
    // disk write takes. Assert the write happens off the calling thread.
    NodeId testKey = new NodeId();
    Updater.setPublicUpdaterKeyForTests(testKey);

    // Must not fall through to the default restartAction (System.exit(0)) — this test's install
    // really does succeed and reach the restart trigger 2s later, on a background thread that
    // outlives this test method. If we returned before it fires, @After's cleanup() would already
    // have reset restartAction back to the System.exit(0) default by the time it does, killing the
    // Surefire fork mid-suite — so, like validUpdate_installs_andInvokesRestartAction, we must wait
    // for it here instead of just no-op-ing and returning early.
    AtomicInteger restartCount = new AtomicInteger();
    InboundCommandProcessor.restartAction = restartCount::incrementAndGet;

    java.util.concurrent.atomic.AtomicReference<Thread> writerThread =
        new java.util.concurrent.atomic.AtomicReference<>();
    InboundCommandProcessor.installThreadHookForTests = writerThread::set;

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

    File updateFile = new File("update");
    awaitCondition(updateFile::exists, 5000);
    awaitCondition(() -> writerThread.get() != null, 5000);

    assertNotEquals(
        callingThread,
        writerThread.get(),
        "the jar write/install must not run on the calling (reader) thread");

    // Wait for the delayed restart trigger too (see comment above) so cleanup() doesn't race it.
    awaitCondition(() -> restartCount.get() == 1, 5000);
    assertEquals(1, restartCount.get());
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

    File apkFile = new File(ConnectionReaderThread.ANDROID_UPDATE_FILE);
    awaitCondition(apkFile::exists, 5000);
    assertTrue(
        java.util.Arrays.equals(data, Files.readAllBytes(apkFile.toPath())),
        "installed apk file must contain the received data");

    awaitCondition(() -> ctx.getLocalSettings().getUpdateAndroidTimestamp() == othersTs, 5000);
    assertTrue(java.util.Arrays.equals(sig, ctx.getLocalSettings().getUpdateAndroidSignature()));
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
    assertFalse(new File("tmp_redpanda.jar").exists(), "tmp file must not be written");
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
    assertFalse(
        new File(ConnectionReaderThread.ANDROID_UPDATE_FILE).exists(),
        "apk file must not be written");
  }

  @Test
  void androidFutureSkew_rejected() {
    long othersTs = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(25);
    byte[] data = new byte[] {3, 3, 3};
    byte[] sig = fakeSignature();

    ByteBuffer in = buildUpdateAnswerContent(othersTs, sig, data);

    int consumed = proc.parseCommand(Command.ANDROID_UPDATE_ANSWER_CONTENT, in, newPeer(8808));

    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
    assertFalse(
        new File(ConnectionReaderThread.ANDROID_UPDATE_FILE).exists(),
        "apk file must not be written");
  }

  @Test
  void androidDowngrade_rejected_belowBuildFloor_onFreshLocalSettings() {
    byte[] data = new byte[] {4, 4, 4, 4};
    byte[] sig = fakeSignature();
    // Not above the compile-time floor -> rejected before any verification.
    ByteBuffer in = buildUpdateAnswerContent(Updater.MIN_UPDATE_TIMESTAMP_MS, sig, data);

    int consumed = proc.parseCommand(Command.ANDROID_UPDATE_ANSWER_CONTENT, in, newPeer(8809));

    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
    assertFalse(
        new File(ConnectionReaderThread.ANDROID_UPDATE_FILE).exists(),
        "apk file must not be written");
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
