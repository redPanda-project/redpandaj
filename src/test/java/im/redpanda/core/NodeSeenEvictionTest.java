package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.security.Security;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Regression test for M5 (bug hunt 2026-07-26): {@code Node.seen(ip, port)} used to {@code break}
 * out of its loop on the first matching connection point, so the two-week staleness scan only ever
 * covered the entries before the match — and never ran at all for a long-lived stable connection,
 * whose point sits at the front of the list and is re-confirmed every two minutes by {@code
 * NodeConnectionPointsSeenJob}. The connection points are persisted with the node, so they
 * accumulated without the intended 14-day bound.
 */
class NodeSeenEvictionTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  void seen_evictsStalePointsBehindTheMatchedOne() {
    Node node = newNode();

    node.addConnectionPoint("10.0.0.1", 1000); // the stable, always-matching point
    node.addConnectionPoint("10.0.0.2", 2000); // stale
    node.addConnectionPoint("10.0.0.3", 3000); // stale
    node.addConnectionPoint("10.0.0.4", 4000); // recent

    backdate(node, "10.0.0.2", Node.CONNECTION_POINT_MAX_AGE_MS + 60_000L);
    backdate(node, "10.0.0.3", Node.CONNECTION_POINT_MAX_AGE_MS + 60_000L);
    backdate(node, "10.0.0.4", Node.CONNECTION_POINT_MAX_AGE_MS - 60_000L);

    node.seen("10.0.0.1", 1000);

    assertThat(ips(node))
        .as("the two-week scan must run past the matched point")
        .containsExactly("10.0.0.1", "10.0.0.4");
  }

  @Test
  void seen_keepsTheMatchedPointEvenIfItWasStale() {
    Node node = newNode();

    node.addConnectionPoint("10.0.0.1", 1000);
    backdate(node, "10.0.0.1", Node.CONNECTION_POINT_MAX_AGE_MS + 60_000L);

    node.seen("10.0.0.1", 1000);

    // the match is refreshed to "now", so it must never be collected by the same pass
    assertThat(ips(node)).containsExactly("10.0.0.1");
    assertThat(node.getConnectionPoints().getFirst().getRetries()).isZero();
  }

  @Test
  void seen_addsAnUnknownPointAndStillEvicts() {
    Node node = newNode();

    node.addConnectionPoint("10.0.0.2", 2000);
    backdate(node, "10.0.0.2", Node.CONNECTION_POINT_MAX_AGE_MS + 60_000L);

    node.seen("10.0.0.9", 9000);

    assertThat(ips(node)).containsExactly("10.0.0.9");
  }

  private static Node newNode() {
    return new Node(ServerContext.buildDefaultServerContext(), new NodeId());
  }

  private static List<String> ips(Node node) {
    return node.getConnectionPoints().stream().map(Node.ConnectionPoint::getIp).toList();
  }

  private static void backdate(Node node, String ip, long ageMs) {
    for (Node.ConnectionPoint point : node.getConnectionPoints()) {
      if (point.getIp().equals(ip)) {
        point.setLastSeen(System.currentTimeMillis() - ageMs);
        return;
      }
    }
    throw new IllegalStateException("no connection point for " + ip);
  }
}
