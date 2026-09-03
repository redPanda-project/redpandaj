package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import im.redpanda.identity.NodeId;
import im.redpanda.ops.Settings;
import im.redpanda.updater.UpdateTransfer;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class InboundCommandProcessorAsyncUpdatesTest {

  private static final String JAR_PATH_PROPERTY = "redpanda.update.jar.path";

  @TempDir public File tempFolder;

  private ServerContext ctx;
  private InboundCommandProcessor proc;

  @BeforeEach
  void setup() {
    ctx = ServerContext.buildDefaultServerContext();
    proc = new InboundCommandProcessor(ctx);
    ByteBufferPool.init();
    // Ensure Settings treat this as non-seed with updates disabled by default
    Settings.seedNode = false;
    Settings.loadUpdates = false;
  }

  @AfterEach
  void cleanup() {
    // Remove files created by tests
    System.clearProperty(JAR_PATH_PROPERTY);
    new File("tmp_redpanda.jar").delete();
    new File(UpdateTransfer.ANDROID_UPDATE_FILE).delete();
  }

  /** A syntactically valid (fixed 64-byte Ed25519) but cryptographically fake signature. */
  private static byte[] fakeSignature() {
    byte[] sig = new byte[NodeId.SIGNATURE_LEN];
    for (int i = 0; i < sig.length; i++) sig[i] = (byte) i;
    return sig;
  }

  @Test
  void updateRequestContent_sendsJarFrame_whenSignaturePresent() throws Exception {
    byte[] data = "jar".getBytes();

    // Point the handler at a private temp-dir jar so this test cannot collide with other
    // Surefire forks sharing the working directory (e.g. SettingsInitTest, which creates and
    // deletes a CWD-relative redpanda.jar).
    Path jarPath = newFile(tempFolder, "redpanda.jar").toPath();
    System.setProperty(JAR_PATH_PROPERTY, jarPath.toString());
    try (FileOutputStream fos = new FileOutputStream(jarPath.toFile())) {
      fos.write(data);
    }

    // Set signature and timestamp to pass initial guards
    ctx.getLocalSettings().setUpdateTimestamp(System.currentTimeMillis());
    ctx.getLocalSettings().setUpdateSignature(fakeSignature());

    Peer peer = new Peer("127.0.0.1", 5555, ctx.getNodeId());
    peer.setConnected(false); // avoid SelectionKey usage in tests
    ctx.getPeerList().add(peer);
    // Big enough buffer; code may grow it, but start with capacity
    peer.writeBuffer = ByteBuffer.allocate(1024);

    int consumed = proc.parseCommand(Command.UPDATE_REQUEST_CONTENT, ByteBuffer.allocate(0), peer);
    assertEquals(1, consumed);

    // The writer thread mutates (and may replace) peer.writeBuffer under writeBufferLock; poll
    // under the same lock for visibility.
    awaitCondition(
        () -> {
          peer.writeBufferLock.lock();
          try {
            return peer.writeBuffer.position() > 0;
          } finally {
            peer.writeBufferLock.unlock();
          }
        },
        5000);

    peer.writeBufferLock.lock();
    try {
      assertEquals(Command.UPDATE_ANSWER_CONTENT, peer.writeBuffer.get(0));
    } finally {
      peer.writeBufferLock.unlock();
    }
  }

  @Test
  void androidUpdateRequestContent_doesNotSend_whenSignatureInvalid() throws Exception {
    // Prepare android.apk data
    byte[] data = "apk".getBytes();
    try (FileOutputStream fos = new FileOutputStream(UpdateTransfer.ANDROID_UPDATE_FILE)) {
      fos.write(data);
    }

    // Set android timestamp and an invalid signature to trigger verify(false) and
    // early return
    ctx.getLocalSettings().setUpdateAndroidTimestamp(System.currentTimeMillis());
    ctx.getLocalSettings().setUpdateAndroidSignature(fakeSignature());

    Peer peer = new Peer("127.0.0.1", 6666, ctx.getNodeId());
    peer.setConnected(false);
    ctx.getPeerList().add(peer);
    peer.writeBuffer = ByteBuffer.allocate(2048);

    int consumed =
        proc.parseCommand(Command.ANDROID_UPDATE_REQUEST_CONTENT, ByteBuffer.allocate(0), peer);
    assertEquals(1, consumed);

    // Wait briefly; verify no bytes were written due to failed verification
    parkFor(300);
    assertEquals(0, peer.writeBuffer.position());
  }

  @Test
  void updateAnswerContent_rejectsFakeSignature() {
    Peer peer = new Peer("127.0.0.1", 7777, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    long newerTs = ctx.getLocalSettings().getUpdateTimestamp() + 1000;
    byte[] data = new byte[] {9, 8, 7, 6};
    byte[] sig = fakeSignature();

    ByteBuffer in = ByteBuffer.allocate(8 + 4 + sig.length + data.length);
    in.putLong(newerTs);
    in.putInt(data.length);
    in.put(sig);
    in.put(data);
    in.flip();

    int consumed = proc.parseCommand(Command.UPDATE_ANSWER_CONTENT, in, peer);
    assertEquals(1 + 8 + 4 + sig.length + data.length, consumed);

    File tmp = new File("tmp_redpanda.jar");
    assertFalse(tmp.exists(), "tmp file should NOT exist because signature is fake");
  }

  private static void awaitCondition(BooleanSupplier condition, long timeoutMillis) {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (System.nanoTime() < deadlineNanos) {
      if (condition.getAsBoolean()) {
        return;
      }
      parkFor(10);
    }
    fail("Condition not met within " + timeoutMillis + "ms");
  }

  private static void parkFor(long millis) {
    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(millis));
  }

  private static File newFile(File parent, String child) throws IOException {
    File result = new File(parent, child);
    result.createNewFile();
    return result;
  }
}
