package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.identity.NodeId;
import org.junit.jupiter.api.Test;

/**
 * Pins {@code ServerContext.ownNodeId} after T118/TD146 turned it from a second field into a value
 * derived from {@link ServerContext#getNodeId()}.
 *
 * <p>Before the change three call sites wrote the pair together:
 *
 * <pre>
 * serverContext.setNodeId(localSettings.getMyIdentity());
 * serverContext.setOwnNodeId(localSettings.getMyIdentity().getKademliaId());
 * </pre>
 *
 * <p>The tests below assert that every reachable setter order still yields exactly the value that
 * second statement produced, that an unset context still reads {@code null}, and that the one
 * behavioural difference is the intended one: a later {@code setNodeId} no longer leaves a stale id
 * behind.
 */
class ServerContextOwnNodeIdTest {

  @Test
  void unsetContextHasNoOwnNodeId() {
    assertThat(new ServerContext().getOwnNodeId()).isNull();
  }

  @Test
  void defaultContextDerivesTheIdentityTheOldPairWrote() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();

    assertThat(serverContext.getOwnNodeId())
        .isEqualTo(serverContext.getLocalSettings().getMyIdentity().getKademliaId())
        .isEqualTo(serverContext.getNodeId().getKademliaId());
  }

  @Test
  void settingTheNodeIdFirstMatchesTheOldTwoStatementIdiom() {
    LocalSettings localSettings = new LocalSettings();
    ServerContext serverContext = new ServerContext();

    serverContext.setNodeId(localSettings.getMyIdentity());

    assertThat(serverContext.getOwnNodeId())
        .isEqualTo(localSettings.getMyIdentity().getKademliaId());
  }

  @Test
  void aLaterNodeIdNoLongerLeavesAStaleOwnNodeId() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    NodeId first = serverContext.getNodeId();
    NodeId second = new NodeId();

    serverContext.setNodeId(second);

    assertThat(serverContext.getOwnNodeId()).isEqualTo(second.getKademliaId());
    assertThat(serverContext.getOwnNodeId()).isNotEqualTo(first.getKademliaId());
  }
}
