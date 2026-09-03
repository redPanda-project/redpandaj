package im.redpanda.routing.graph;

import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.ops.Job;
import java.time.Duration;

public class NodeConnectionPointsSeenJob extends Job {

  public NodeConnectionPointsSeenJob(ServerContext serverContext) {
    super(serverContext, Duration.ofMinutes(2).toMillis(), true, true);
  }

  @Override
  public void init() {
    // nothing to do
  }

  @Override
  public void work() {

    for (Peer peer : serverContext.getPeerList().snapshot()) {
      // Resolve the node once: Peer.getNode() returns null as soon as the peer loses `connected`
      // or `authed`, which a reader thread can do between the check and the call (Copilot review
      // on this PR). Nothing here ever guarded that -- the peer list lock this loop used to hold
      // guards the list structure, not the peers in it.
      Node node = peer.getNode();
      if (peer.isConnected() && node != null) {
        node.seen(peer.getIp(), peer.getPort());
      }
    }
  }
}
