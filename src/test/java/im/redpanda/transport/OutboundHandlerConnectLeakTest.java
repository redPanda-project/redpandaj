package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.UnixOperatingSystemMXBean;
import im.redpanda.core.ServerContext;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/**
 * Regression test for H4 (bug hunt 2026-07-26): {@code OutboundHandler.connectTo} opened a
 * SocketChannel and only logged when the subsequent connect attempt threw, leaking one file
 * descriptor per attempt. {@code run()} retries unreachable peers continuously, so this accumulated
 * until FD exhaustion.
 */
class OutboundHandlerConnectLeakTest {

  private static final int ATTEMPTS = 50;

  @Test
  void connectTo_doesNotLeakSocketChannelWhenConnectFails() throws Exception {
    OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
    assumeTrue(
        osBean instanceof UnixOperatingSystemMXBean,
        "open file descriptor count is not available on this platform");
    UnixOperatingSystemMXBean unixBean = (UnixOperatingSystemMXBean) osBean;

    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    Method connectTo =
        OutboundHandler.class.getDeclaredMethod("connectTo", ServerContext.class, Peer.class);
    connectTo.setAccessible(true);

    // An out-of-range port makes the InetSocketAddress constructor throw IllegalArgumentException
    // right after SocketChannel.open() succeeded — exactly the window in which the channel leaked.
    connectTo.invoke(null, serverContext, newUnconnectablePeer()); // warm up

    long openFdsBefore = unixBean.getOpenFileDescriptorCount();
    for (int i = 0; i < ATTEMPTS; i++) {
      assertThat(connectTo.invoke(null, serverContext, newUnconnectablePeer())).isEqualTo(false);
    }
    long leaked = unixBean.getOpenFileDescriptorCount() - openFdsBefore;

    assertThat(leaked)
        .as("failed connect attempts must not accumulate file descriptors")
        .isLessThan(ATTEMPTS / 2L);
  }

  private static Peer newUnconnectablePeer() {
    return new Peer("127.0.0.1", 70000);
  }
}
