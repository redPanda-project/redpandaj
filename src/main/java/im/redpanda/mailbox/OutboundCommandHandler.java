package im.redpanda.mailbox;

import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.core.Peer;
import im.redpanda.outbound.v1.AckFetchRequest;
import im.redpanda.outbound.v1.FetchRequest;
import im.redpanda.outbound.v1.RegisterOhRequest;
import im.redpanda.outbound.v1.RevokeOhRequest;
import im.redpanda.outbound.v1.SubscribeRequest;

/**
 * Outbound-V1 request frames (commands 150/152/154/156/159): parse the protobuf and hand it to
 * {@link OutboundService}.
 *
 * <p>Split out of {@code core.InboundCommandProcessor} by T116 (DDD review 2026-08-31, §6 P2 step
 * 2) so the {@code im.redpanda.outbound.v1} protobuf types stop leaking into the wire dispatcher.
 * The dispatcher registers all five as framed commands, so it has already read the whole {@code
 * [len:4][payload]} frame and the payload handed in here is never {@code null}; the consumed-byte
 * accounting is the dispatcher's, not this class's.
 *
 * <p>The response commands 151/153/155/157/158/160/161 are written by {@link OutboundService} and
 * never parsed on this side.
 */
public class OutboundCommandHandler {

  private final OutboundService outboundService;

  public OutboundCommandHandler(OutboundService outboundService) {
    this.outboundService = outboundService;
  }

  /** Command 150: register an outbound handle (mailbox lease). */
  public void handleRegister(Peer peer, byte[] payload) throws InvalidProtocolBufferException {
    outboundService.handleRegister(peer, RegisterOhRequest.parseFrom(payload));
  }

  /** Command 152: fetch queued mailbox items. */
  public void handleFetch(Peer peer, byte[] payload) throws InvalidProtocolBufferException {
    outboundService.handleFetch(peer, FetchRequest.parseFrom(payload));
  }

  /** Command 154: revoke an outbound handle. */
  public void handleRevoke(Peer peer, byte[] payload) throws InvalidProtocolBufferException {
    outboundService.handleRevoke(peer, RevokeOhRequest.parseFrom(payload));
  }

  /** Command 156: commit the fetch cursor. */
  public void handleAckFetch(Peer peer, byte[] payload) throws InvalidProtocolBufferException {
    outboundService.handleAckFetch(peer, AckFetchRequest.parseFrom(payload));
  }

  /** Command 159 (T38 connection-notify): opt-in subscribe. */
  public void handleSubscribe(Peer peer, byte[] payload) throws InvalidProtocolBufferException {
    outboundService.handleSubscribe(peer, SubscribeRequest.parseFrom(payload));
  }
}
