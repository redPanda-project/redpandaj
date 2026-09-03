package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the retention fix for T86.
 *
 * <p>Every inbound connection puts a {@link Peer} into the peer list. The handshake carries the
 * sender's listening port, which is 0 for a light client, so those entries are undialable — and
 * nothing ever removed them again, because {@code OutboundHandler} skips undialable peers before it
 * reaches its retry-based eviction. On the affected node that produced 273 undialable entries out
 * of 278, one per mobile app instance, per re-install and per e2e run, persisted to the {@code
 * peers.json} file and gossiped on.
 */
class PeerJobsUndialablePeerEvictionTest {

  @AfterEach
  void tearDown() {
    ConnectionHandler.peerInHandshakes.clear();
  }

  private ServerContext context() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    serverContext.setConnectionHandler(new ConnectionHandler(serverContext, false));
    ConnectionHandler.peerInHandshakes.clear();
    return serverContext;
  }

  /**
   * An inbound peer as ConnectionHandler.setupConnection builds it: remote ip, announced port 0.
   */
  private Peer inboundPeer(String ip) {
    Peer peer = new Peer(ip, 0);
    peer.setNodeId(NodeId.generateWithSimpleKey());
    return peer;
  }

  @Test
  void runOnce_dropsDisconnectedUndialablePeers() {
    ServerContext ctx = context();
    Peer lightClient = inboundPeer("84.147.60.253");
    Peer loopbackLightClient = inboundPeer("127.0.0.1");
    ctx.getPeerList().add(lightClient);
    ctx.getPeerList().add(loopbackLightClient);

    new PeerJobs(ctx).runOnce();

    assertThat(ctx.getPeerList().size()).isZero();
  }

  @Test
  void runOnce_keepsTheLiveInboundConnection() {
    ServerContext ctx = context();
    Peer connectedLightClient = inboundPeer("84.147.60.253");
    connectedLightClient.setConnected(true);
    ctx.getPeerList().add(connectedLightClient);

    new PeerJobs(ctx).runOnce();

    assertThat(ctx.getPeerList().contains(connectedLightClient.getKademliaId()))
        .as("a connected inbound peer is the one thing such an entry is good for")
        .isTrue();
  }

  @Test
  void runOnce_keepsDialablePeersEvenWhileDisconnected() {
    ServerContext ctx = context();
    Peer node = new Peer("46.224.156.238", 59558);
    node.setNodeId(NodeId.generateWithSimpleKey());
    Peer loopbackNode = new Peer("127.0.0.1", 59560); // e2e topology: real port on loopback
    loopbackNode.setNodeId(NodeId.generateWithSimpleKey());
    ctx.getPeerList().add(node);
    ctx.getPeerList().add(loopbackNode);

    new PeerJobs(ctx).runOnce();

    assertThat(ctx.getPeerList().contains(node.getKademliaId())).isTrue();
    assertThat(ctx.getPeerList().contains(loopbackNode.getKademliaId())).isTrue();
  }

  @Test
  void isDialableRejectsMissingConnectionDetails() {
    Peer peer = new Peer("46.224.156.238", 59558);
    assertThat(peer.isDialable()).isTrue();
    peer.removeIpAndPort();
    assertThat(peer.isDialable()).isFalse();
  }
}
