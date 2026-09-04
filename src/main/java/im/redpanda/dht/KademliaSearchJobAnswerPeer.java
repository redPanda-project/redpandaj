package im.redpanda.dht;

import static com.google.protobuf.ByteString.copyFrom;

import im.redpanda.core.Command;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.proto.KademliaGetAnswer;
import im.redpanda.transport.Peer;
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KademliaSearchJobAnswerPeer extends KademliaSearchJob {

  private static final Logger logger = LogManager.getLogger();

  private final Peer answerTo;
  private final int ackID;

  public KademliaSearchJobAnswerPeer(
      ServerContext serverContext, KademliaId id, Peer answerTo, int ackID) {
    super(serverContext, id);
    this.answerTo = answerTo;
    this.ackID = ackID;
  }

  @Override
  protected ArrayList<KadContent> success() {

    ArrayList<KadContent> kadContents = super.success();

    if (kadContents == null || kadContents.getFirst() == null) {
      logger.debug("kademlia answer job failed, did not find an entry in time");
      fail();
      return null;
    }

    if (!answerTo.isConnected()) {
      return kadContents;
    }

    /** write the least 3 newst entries... */
    for (int i = 0; i < Math.min(3, kadContents.size()); i++) {

      KadContent kadContent = kadContents.get(i);

      var answerMsg =
          KademliaGetAnswer.newBuilder()
              .setAckId(ackID)
              .setTimestamp(kadContent.getTimestamp())
              .setPublicKey(copyFrom(kadContent.getPubkey()))
              .setContent(copyFrom(kadContent.getContent()))
              .setSignature(copyFrom(kadContent.getSignature()))
              .build();

      // TD110: the peer can disconnect between the isConnected() check above and here, and a
      // dropped answer used to leave the requester waiting for its whole job timeout with no
      // trace on our side. Stop after the first drop: the remaining frames would go to the same
      // gone peer.
      if (!answerTo.enqueueFrame(Command.KADEMLIA_GET_ANSWER, answerMsg.toByteArray())) {
        logger.debug(
            "could not queue KADEMLIA_GET_ANSWER for {}: peer already disconnected, dropping the"
                + " remaining {} answer(s)",
            answerTo,
            Math.min(3, kadContents.size()) - i - 1);
        break;
      }
    }

    return kadContents;
  }
}
