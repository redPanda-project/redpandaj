package im.redpanda.routing;

import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.core.ServerContext;
import im.redpanda.mailbox.MailboxDepositPolicy;
import im.redpanda.mailbox.OutboundService;
import im.redpanda.proto.FlaschenpostPut;
import im.redpanda.transport.Peer;

/**
 * The mailbox/garlic inbound wire commands: 141 (FLASCHENPOST_PUT) and 142 (FLASCHENPOST_V2).
 *
 * <p>Split out of {@code core.InboundCommandProcessor} by T116 (DDD review 2026-08-31, §6 P2 step
 * 2). Both are parse-and-delegate: the deposit/forward/R-ACK policy has been owned by {@link
 * MailboxDepositPolicy} since T108, and the multi-hop relay by {@link GarlicRouter}. This class
 * exists so the protobuf parse of those two frames lives in the domain that defines them instead of
 * in the wire dispatcher.
 */
public class FlaschenpostCommandHandler {

  private final ServerContext serverContext;
  private final OutboundService outboundService;

  public FlaschenpostCommandHandler(ServerContext serverContext, OutboundService outboundService) {
    this.serverContext = serverContext;
    this.outboundService = outboundService;
  }

  /** Command 141: parse the FLASCHENPOST_PUT frame and hand it to the mailbox domain. */
  public void handlePut(Peer peer, byte[] payload) throws InvalidProtocolBufferException {
    MailboxDepositPolicy.handlePut(
        serverContext, outboundService, FlaschenpostPut.parseFrom(payload), peer);
  }

  /**
   * Command 142: MS04 multi-hop garlic relay — dedup, peel our own layer or route toward next_hop.
   */
  public void handleV2(byte[] payload) {
    GarlicRouter.handle(serverContext, payload);
  }
}
