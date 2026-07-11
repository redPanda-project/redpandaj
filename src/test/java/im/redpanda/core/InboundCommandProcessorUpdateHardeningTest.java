package im.redpanda.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers the T10 (updater option B) hardening added to the UPDATE_ANSWER_CONTENT handler: rollback
 * protection via a build-time floor, a future-timestamp skew guard, and fail-closed behaviour when
 * no updater key is configured. See PLAN-updater-option-b.md, PR 1.
 */
public class InboundCommandProcessorUpdateHardeningTest {

  private static final int TEST_PORT = 49781;

  private ServerContext ctx;
  private InboundCommandProcessor proc;

  @Before
  public void setup() {
    ctx = ServerContext.buildDefaultServerContext();
    ctx.setPort(TEST_PORT);
    proc = new InboundCommandProcessor(ctx);
    ByteBufferPool.init();
    Settings.seedNode = false;
    Settings.loadUpdates = false;
  }

  @After
  public void cleanup() {
    Updater.resetPublicUpdaterKeyForTests();
    InboundCommandProcessor.restartAction = () -> System.exit(0);
    new File("tmp_redpanda.jar").delete();
    new File("update").delete();
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
  public void downgrade_rejected_whenTimestampNotAboveLocal() {
    long localTs = Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L;
    ctx.getLocalSettings().setUpdateTimestamp(localTs);

    byte[] data = new byte[] {1, 2, 3};
    byte[] sig = fakeSignature();
    // othersTimestamp == localTs -> not strictly greater -> rejected as a downgrade/replay.
    ByteBuffer in = buildUpdateAnswerContent(localTs, sig, data);

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, newPeer(8801));

    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
    assertFalse("tmp file must not be written", new File("tmp_redpanda.jar").exists());
    assertFalse("update file must not be written", new File("update").exists());
    assertEquals(localTs, ctx.getLocalSettings().getUpdateTimestamp());
  }

  @Test
  public void downgrade_rejected_belowBuildFloor_onFreshLocalSettings() {
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
    assertFalse("tmp file must not be written", new File("tmp_redpanda.jar").exists());
    assertFalse("update file must not be written", new File("update").exists());
    assertEquals(-1L, ctx.getLocalSettings().getUpdateTimestamp());
  }

  @Test
  public void futureSkew_rejected() {
    long othersTs = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(25);
    byte[] data = new byte[] {1, 1, 1};
    byte[] sig = fakeSignature();

    ByteBuffer in = buildUpdateAnswerContent(othersTs, sig, data);

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, newPeer(8803));

    assertEquals(
        "handler must report exactly the consumed byte length or the parser desyncs",
        1 + 8 + 4 + sig.length + data.length,
        consumed);
    assertFalse("tmp file must not be written", new File("tmp_redpanda.jar").exists());
    assertFalse("update file must not be written", new File("update").exists());
  }

  @Test
  public void placeholderKey_failClosed_noFileWritten() {
    // Force the fail-closed state via the test override instead of relying on the (now real,
    // non-placeholder) PUBLIC_SIGNING_KEY_OF_CORE_DEVELOPERS constant.
    Updater.setPublicUpdaterKeyForTests(null);

    byte[] data = new byte[] {2, 4, 6, 8};
    long othersTs = Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L;
    byte[] sig = fakeSignature();

    ByteBuffer in = buildUpdateAnswerContent(othersTs, sig, data);

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, newPeer(8804));

    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);
    assertFalse("tmp file must not be written", new File("tmp_redpanda.jar").exists());
    assertFalse("update file must not be written", new File("update").exists());
    assertEquals(-1L, ctx.getLocalSettings().getUpdateTimestamp());
  }

  @Test
  public void validUpdate_installs_andInvokesRestartAction() throws Exception {
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
        "installed update file must contain the received data",
        java.util.Arrays.equals(data, Files.readAllBytes(updateFile.toPath())));

    assertEquals(othersTs, ctx.getLocalSettings().getUpdateTimestamp());
    assertTrue(java.util.Arrays.equals(sig, ctx.getLocalSettings().getUpdateSignature()));

    // The restart happens 2s after installing, asynchronously; wait for it and make sure the
    // JVM (this test process) is still alive to observe it.
    awaitCondition(() -> restartCount.get() == 1, 5000);
    assertEquals(1, restartCount.get());
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
