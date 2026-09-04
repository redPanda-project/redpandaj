package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import java.nio.ByteBuffer;
import java.security.Security;
import org.junit.jupiter.api.Test;

class RequestPeerListJobTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /**
   * {@code PeerList.getGoodPeer(1.0f)} returns null on an empty peer list. That used to NPE into
   * the job's broad catch, which logged "Error requesting peerlist" without the exception — a fresh
   * node with no peers yet reported an error every 30 seconds for something that is not one.
   */
  @Test
  void work_withEmptyPeerList_doesNotThrow() {
    ServerContext ctx = ServerContext.buildDefaultServerContext();
    assertThat(ctx.getPeerList().size()).isZero();

    assertThatCode(() -> new RequestPeerListJob(ctx).work()).doesNotThrowAnyException();
  }

  @Test
  void work_queuesRequestPeerListOnTheChosenPeer() {
    ServerContext ctx = ServerContext.buildDefaultServerContext();
    Peer peer = new Peer("127.0.0.1", 15321, new NodeId());
    peer.setConnected(true);
    peer.writeBuffer = ByteBuffer.allocate(16);
    ctx.getPeerList().add(peer);

    new RequestPeerListJob(ctx).work();

    assertThat(peer.writeBuffer.position()).isEqualTo(1);
    assertThat(peer.writeBuffer.get(0)).isEqualTo(im.redpanda.core.Command.REQUEST_PEERLIST);
  }
}
