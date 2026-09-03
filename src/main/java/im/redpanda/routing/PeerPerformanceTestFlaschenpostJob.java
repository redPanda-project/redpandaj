package im.redpanda.routing;

import static com.google.protobuf.ByteString.copyFrom;

import im.redpanda.core.Command;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import im.redpanda.ops.Job;
import im.redpanda.proto.FlaschenpostPut;

public class PeerPerformanceTestFlaschenpostJob extends Job {

  Peer peer;
  boolean success = false;

  public PeerPerformanceTestFlaschenpostJob(ServerContext serverContext, Peer peer) {
    super(serverContext);
    this.peer = peer;
  }

  @Override
  public void init() {

    peer.getNode().cleanChecks();

    System.out.println("we are creating a Flaschenpost to monitor other peers...");

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
    peer.enqueueFrame(Command.FLASCHENPOST_PUT, putMsg.toByteArray());
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
