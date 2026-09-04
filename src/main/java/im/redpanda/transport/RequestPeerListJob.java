package im.redpanda.transport;

import im.redpanda.core.Command;
import im.redpanda.core.ServerContext;
import im.redpanda.ops.Job;
import im.redpanda.ops.Log;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RequestPeerListJob extends Job {

  private static final Logger logger = LogManager.getLogger();

  public RequestPeerListJob(ServerContext serverContext) {
    super(serverContext, 1000L * 30L * 1L, true);
  }

  @Override
  public void init() {}

  @Override
  public void work() {

    // todo request and send peers over garlic messages...

    try {
      Peer peer = serverContext.getPeerList().getGoodPeer(1.0f);
      if (peer == null) {
        // Empty peer list: nothing to ask this round. This used to NPE straight into the catch
        // below, where it was logged as "Error requesting peerlist" with no exception attached.
        logger.debug("no peer available to request a peer list from");
        return;
      }
      // TD110: enqueueCommand reports "peer is gone" instead of throwing (T115). Without this
      // branch a peer that disconnected between getGoodPeer() and here silently swallowed the
      // request and we simply never refreshed the peer list this round.
      if (!peer.enqueueCommand(Command.REQUEST_PEERLIST)) {
        logger.debug("could not queue REQUEST_PEERLIST for {}: peer already disconnected", peer);
      }
    } catch (Exception e) {
      // Keep the legacy line (log level 100 -> debug) and add the cause, which was dropped.
      Log.put("Error requesting peerlist", 100);
      logger.debug("error requesting peerlist", e);
    }
  }
}
