package im.redpanda.dht;

import static com.google.protobuf.ByteString.copyFrom;

import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.core.Command;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.proto.JobAck;
import im.redpanda.proto.KademliaGet;
import im.redpanda.proto.KademliaGetAnswer;
import im.redpanda.proto.KademliaStore;
import im.redpanda.transport.Peer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The DHT's inbound wire commands: 120 (KADEMLIA_STORE), 121 (KADEMLIA_GET), 122
 * (KADEMLIA_GET_ANSWER) and 130 (JOB_ACK, the insert-job correlation ack).
 *
 * <p>Split verbatim out of {@code core.InboundCommandProcessor} by T116 (DDD review 2026-08-31, §6
 * P2 step 2): the dispatcher parses the frame, the DHT owns what the frame means. Nothing here
 * changed behaviourally.
 *
 * <p>The {@code jobRegistry.get(id) instanceof …} correlation is the hand-rolled pending-
 * conversation registry the review's §"Fehlende Konzepte" flags; it moves as-is and is not this
 * task's business.
 */
public class KademliaCommandHandler {

  private static final Logger logger = LogManager.getLogger();

  private final ServerContext serverContext;

  public KademliaCommandHandler(ServerContext serverContext) {
    this.serverContext = serverContext;
  }

  public void handleJobAck(Peer peer, byte[] payload) throws InvalidProtocolBufferException {
    JobAck ackMsg = JobAck.parseFrom(payload);
    int jobId = ackMsg.getJobId();
    var runningJob = serverContext.getJobRegistry().get(jobId);
    if (runningJob instanceof KademliaInsertJob job) {
      job.ack(peer);
      // TD133: this was a System.out.println with peer.getNodeId().toString() on it. A light
      // client that never sent a public key has a null NodeId, so an ACK from one NPEd out of the
      // dispatcher — the log4j placeholder renders "null" instead.
      logger.debug("ACK for job {} from peer {} (node id {})", jobId, peer, peer.getNodeId());
    }
  }

  public void handleKademliaGet(Peer peer, byte[] payload) throws InvalidProtocolBufferException {
    KademliaGet getMsg = KademliaGet.parseFrom(payload);
    int jobId = getMsg.getJobId();
    var searchedId = new KademliaId(getMsg.getSearchedId().getKeyBytes().toByteArray());
    var kadContent = serverContext.getKadStoreManager().get(searchedId);
    if (kadContent != null) {
      var answerMsg =
          KademliaGetAnswer.newBuilder()
              .setAckId(jobId)
              .setTimestamp(kadContent.getTimestamp())
              .setPublicKey(copyFrom(kadContent.getPubkey()))
              .setContent(copyFrom(kadContent.getContent()))
              .setSignature(copyFrom(kadContent.getSignature()))
              .build();
      if (!peer.enqueueFrame(Command.KADEMLIA_GET_ANSWER, answerMsg.toByteArray())) {
        logger.debug("could not queue KADEMLIA_GET_ANSWER for {}: peer already disconnected", peer);
      }
    } else {
      new KademliaSearchJobAnswerPeer(serverContext, searchedId, peer, jobId).start();
    }
  }

  public void handleKademliaStore(Peer peer, byte[] payload) throws InvalidProtocolBufferException {
    KademliaStore storeMsg = KademliaStore.parseFrom(payload);
    int jobId = storeMsg.getJobId();
    var kadContent =
        new KadContent(
            storeMsg.getTimestamp(),
            storeMsg.getPublicKey().toByteArray(),
            storeMsg.getContent().toByteArray(),
            storeMsg.getSignature().toByteArray());
    if (kadContent.verify()) {
      serverContext.getKadStoreManager().put(kadContent);
      if (jobId != 0) {
        var ackMsg = JobAck.newBuilder().setJobId(jobId).build();
        if (!peer.enqueueFrame(Command.JOB_ACK, ackMsg.toByteArray())) {
          logger.debug("could not queue JOB_ACK for {}: peer already disconnected", peer);
        }
      }
    } else {
      logger.error("Kademlia content verification failed!");
    }
  }

  public void handleKademliaGetAnswer(Peer peer, byte[] payload)
      throws InvalidProtocolBufferException {
    KademliaGetAnswer answerMsg = KademliaGetAnswer.parseFrom(payload);
    var kadContent =
        new KadContent(
            answerMsg.getTimestamp(),
            answerMsg.getPublicKey().toByteArray(),
            answerMsg.getContent().toByteArray(),
            answerMsg.getSignature().toByteArray());
    if (kadContent.verify()) {
      var byId = serverContext.getJobRegistry().get(answerMsg.getAckId());
      if (byId instanceof KademliaSearchJob job) {
        job.ack(kadContent, peer);
      }
    } else {
      logger.error("Kademlia content verification failed!");
    }
  }
}
