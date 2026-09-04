package im.redpanda.routing;

import static com.google.protobuf.ByteString.copyFrom;

import im.redpanda.core.Command;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import im.redpanda.ops.Job;
import im.redpanda.proto.FlaschenpostPut;
import im.redpanda.transport.Peer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PeerPerformanceTestFlaschenpostJob extends Job {

  private static final Logger logger = LogManager.getLogger();

  Peer peer;
  boolean success = false;

  public PeerPerformanceTestFlaschenpostJob(ServerContext serverContext, Peer peer) {
    super(serverContext);
    this.peer = peer;
  }

  @Override
  public void init() {

    peer.getNode().cleanChecks();

    logger.debug("creating a flaschenpost to monitor other peers");

    // lets target to ourselves without the private key!
    NodeId targetId = NodeId.importPublic(serverContext.getNodeId().exportPublic());

    GMAck gmAck = new GMAck(getJobId());

    GarlicMessage garlicMessage = new GarlicMessage(serverContext, targetId);
    garlicMessage.addGMContent(gmAck);

    byte[] content = garlicMessage.getContent();

    if (!peer.isConnected() || peer.getNode() == null) {
      return;
    }

    var putMsg = FlaschenpostPut.newBuilder().setContent(copyFrom(content)).build();
    // TD110: a drop here means the probe never leaves, so the job can only time out and count the
    // peer as failed. Say so instead of scoring the peer down without a trace.
    if (!peer.enqueueFrame(Command.FLASCHENPOST_PUT, putMsg.toByteArray())) {
      logger.debug("peer {} is gone, flaschenpost performance probe not sent", peer);
    }
  }

  @Override
  public void work() {}

  @Override
  public void done() {
    super.done();
    if (success) {
      peer.getNode().increaseGmTestsSuccessful();
    } else {
      peer.getNode().increaseGmTestsFailed();
    }
  }

  public void success() {
    success = true;
    done();
  }
}
