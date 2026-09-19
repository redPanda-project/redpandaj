package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.routing.graph.NodeStore;
import java.security.Security;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * TD223: {@code Server.shutdown()} runs twice on a job-triggered restart. {@code
 * ServerRestartJob.work()} calls it and its following {@code System.exit(0)} runs the JVM shutdown
 * hook of {@code App}, which calls it again — a second {@code savePeers} plus {@code
 * localSettings.save} against an already closed {@code NodeStore}. {@code ListenConsole}'s {@code
 * e} command has the same pairing. It must be idempotent instead.
 */
class ServerShutdownIdempotencyTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /**
   * Counts {@code save()} instead of writing the settings file: what matters here is how often the
   * shutdown path saves, not what it writes (that is {@code LocalSettingsPersistenceTest}).
   */
  private static class CountingLocalSettings extends LocalSettings {
    final AtomicInteger saves = new AtomicInteger();

    @Override
    public synchronized void save(int port) {
      saves.incrementAndGet();
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

  private static ServerContext contextWith(CountingLocalSettings settings) {
    ServerContext serverContext = new ServerContext();
    serverContext.setPort(-1);
    serverContext.setLocalSettings(settings);
    serverContext.setNodeId(settings.getMyIdentity());
    serverContext.setNodeStore(NodeStore.buildWithMemoryCacheOnly(serverContext));
    return serverContext;
  }

  @Test
  void theSecondShutdownIsANoOp() {
    CountingLocalSettings settings = new CountingLocalSettings();
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
   * A harness that runs several node lifecycles in one JVM clears the flag between them ({@code
   * TestNodeLauncher.configureSettings()}); that must re-arm the shutdown, or the second node would
   * never save its state.
   */
  @Test
  void clearingTheShuttingDownFlagReArmsShutdown() {
    CountingLocalSettings first = new CountingLocalSettings();
    Server.setShuttingDown(false);
    Server.shutdown(contextWith(first));
    assertThat(first.saves).hasValue(1);

    CountingLocalSettings second = new CountingLocalSettings();
    Server.setShuttingDown(false);
    Server.shutdown(contextWith(second));
    assertThat(second.saves).hasValue(1);
  }
}
