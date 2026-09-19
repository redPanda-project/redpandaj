package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import im.redpanda.routing.graph.NodeStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * TD223: {@code Server.shutdown()} runs twice on a job-triggered restart. {@code
 * ServerRestartJob.work()} calls it and its following {@code System.exit(0)} runs the JVM shutdown
 * hook of {@code App}, which calls it again — a second {@code savePeers} plus {@code
 * localSettings.save} against an already closed {@code NodeStore}. {@code ListenConsole}'s {@code
 * e} command has the same pairing. It must be idempotent instead — but idempotent in the two ways
 * the adversarial review of this PR insisted on: a shutdown that <b>threw</b> must not count as
 * done, and a caller arriving while one is in flight must wait instead of letting the JVM halt
 * mid-save.
 *
 * <p>These tests drive the real {@code Server.shutdown()}, which has two process-global effects
 * this class has to clean up after: it sets the static {@code shuttingDown} flag that every polling
 * loop in this fork reads ({@code @AfterEach} clears it again), and it writes the peers file of the
 * working directory (backed up and restored around the class).
 */
class ServerShutdownIdempotencyTest {

  private static final Path PEERS_FILE = Path.of("data", "peers.json");
  private static byte[] peersFileBackup;

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /**
   * Counts {@code save()} instead of writing the settings file, and can be scripted to fail or to
   * park inside the save. What matters here is how often and when the shutdown path saves, not what
   * it writes (that is {@code LocalSettingsPersistenceTest}).
   */
  private static class ScriptedLocalSettings extends LocalSettings {
    final AtomicInteger saves = new AtomicInteger();
    volatile boolean failNextSave = false;
    volatile CountDownLatch enteredSave = null;
    volatile CountDownLatch releaseSave = null;

    @Override
    public synchronized void save(int port) {
      saves.incrementAndGet();
      if (enteredSave != null) {
        enteredSave.countDown();
      }
      if (releaseSave != null) {
        try {
          releaseSave.await(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (failNextSave) {
        failNextSave = false;
        throw new IllegalStateException("TD223: save failed on purpose");
      }
    }
  }

  @BeforeAll
  static void backUpPeersFile() throws IOException {
    if (Files.exists(PEERS_FILE)) {
      peersFileBackup = Files.readAllBytes(PEERS_FILE);
    }
  }

  @AfterAll
  static void restorePeersFile() throws IOException {
    if (peersFileBackup != null) {
      Files.write(PEERS_FILE, peersFileBackup);
    }
  }

  /**
   * {@code shuttingDown} is process-global state every other thread polls, so it must not leak into
   * the next test of this fork. Clearing it also re-arms {@code shutdown()}.
   */
  @AfterEach
  void clearShutdownState() {
    Server.setShuttingDown(false);
  }

  private static ServerContext contextWith(ScriptedLocalSettings settings) {
    ServerContext serverContext = new ServerContext();
    serverContext.setPort(-1);
    serverContext.setLocalSettings(settings);
    serverContext.setNodeId(settings.getMyIdentity());
    serverContext.setNodeStore(NodeStore.buildWithMemoryCacheOnly(serverContext));
    return serverContext;
  }

  @Test
  void theSecondShutdownIsANoOp() {
    ScriptedLocalSettings settings = new ScriptedLocalSettings();
    ServerContext serverContext = contextWith(settings);
    Server.setShuttingDown(false);

    Server.shutdown(serverContext);
    assertThat(settings.saves).hasValue(1);
    assertThat(Server.isShuttingDown()).isTrue();

    // this is the JVM hook running after ServerRestartJob's System.exit(0)
    Server.shutdown(serverContext);
    assertThat(settings.saves).hasValue(1);
  }

  /**
   * A shutdown that threw did not persist everything, so the next caller must try again. With a
   * "claimed" flag instead of a "completed" one, a failing save (full disk) would have left the
   * node without a settings save at all — on exactly the error path this PR's shutdown work is
   * about.
   */
  @Test
  void aFailedShutdownIsRetriedByTheNextCaller() {
    ScriptedLocalSettings settings = new ScriptedLocalSettings();
    ServerContext serverContext = contextWith(settings);
    Server.setShuttingDown(false);
    settings.failNextSave = true;

    assertThatThrownBy(() -> Server.shutdown(serverContext))
        .isInstanceOf(IllegalStateException.class);
    assertThat(settings.saves).hasValue(1);

    // the JVM hook after ServerRestartJob's System.exit: it must retry, not skip
    Server.shutdown(serverContext);
    assertThat(settings.saves).hasValue(2);
  }

  /**
   * A second caller must wait for an in-flight shutdown rather than return at once: if the JVM hook
   * returned while {@code ServerRestartJob}'s shutdown was still inside {@code savePeers} or {@code
   * NodeStore.close()}, the JVM would halt as soon as all hooks are done and kill that thread mid
   * write.
   */
  @Test
  void aConcurrentCallerWaitsForTheShutdownInFlight() throws Exception {
    ScriptedLocalSettings settings = new ScriptedLocalSettings();
    ServerContext serverContext = contextWith(settings);
    Server.setShuttingDown(false);
    settings.enteredSave = new CountDownLatch(1);
    settings.releaseSave = new CountDownLatch(1);

    Thread first = new Thread(() -> Server.shutdown(serverContext), "shutdown-first");
    first.start();
    assertThat(settings.enteredSave.await(20, TimeUnit.SECONDS)).isTrue();

    AtomicBoolean secondReturned = new AtomicBoolean(false);
    Thread second =
        new Thread(
            () -> {
              Server.shutdown(serverContext);
              secondReturned.set(true);
            },
            "shutdown-second");
    second.start();

    // no sleep: the second caller must end up BLOCKED on the shutdown monitor
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (second.getState() != Thread.State.BLOCKED && System.nanoTime() < deadlineNanos) {
      Thread.onSpinWait();
    }
    assertThat(second.getState()).isEqualTo(Thread.State.BLOCKED);
    assertThat(secondReturned).isFalse();

    settings.releaseSave.countDown();
    first.join(TimeUnit.SECONDS.toMillis(20));
    second.join(TimeUnit.SECONDS.toMillis(20));

    assertThat(secondReturned).isTrue();
    assertThat(settings.saves).as("the waiting caller must not save a second time").hasValue(1);
  }

  /**
   * A harness that runs several node lifecycles in one JVM clears the flag between them ({@code
   * TestNodeLauncher.configureSettings()}); that must re-arm the shutdown, or the second node would
   * never save its state.
   */
  @Test
  void clearingTheShuttingDownFlagReArmsShutdown() {
    ScriptedLocalSettings first = new ScriptedLocalSettings();
    Server.setShuttingDown(false);
    Server.shutdown(contextWith(first));
    assertThat(first.saves).hasValue(1);

    ScriptedLocalSettings second = new ScriptedLocalSettings();
    Server.setShuttingDown(false);
    Server.shutdown(contextWith(second));
    assertThat(second.saves).hasValue(1);
  }
}
