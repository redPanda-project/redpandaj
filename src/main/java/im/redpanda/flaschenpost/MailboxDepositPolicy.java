package im.redpanda.flaschenpost;

import com.google.protobuf.ByteString;
import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.ServerContext;
import im.redpanda.outbound.OhId;
import im.redpanda.outbound.OutboundMailboxStore;
import im.redpanda.outbound.OutboundService;
import im.redpanda.outbound.v1.Status;
import im.redpanda.proto.FlaschenpostPut;
import lombok.extern.slf4j.Slf4j;

/**
 * Deposit / forward / routing-ack policy for an inbound {@code FLASCHENPOST_PUT} (MS01–MS06).
 *
 * <p>Extracted from {@code InboundCommandProcessor.handleFlaschenpostPut} (DDD review 2026-08-31,
 * §4/§6 Top-3): the wire parser owned the whole mailbox-domain policy — which packet is deposited
 * locally, which is forwarded toward the OH host, when a routing ack (R-ACK) is emitted and which
 * status a light client sees. The parser now only parses the frame and delegates here; this class
 * holds the policy and is wire-invariant (same bytes, same order, same log effects).
 *
 * <p><b>Placement:</b> the review names {@code OutboundService} as the target. Everything this
 * policy orchestrates besides the deposit itself ({@link OhForwarder}, {@link RoutingAckSender},
 * {@link ReturnPath}, {@link GMParser}) lives in this package, and {@code im.redpanda.outbound}
 * currently imports nothing from {@code im.redpanda.flaschenpost} — moving the policy into {@code
 * OutboundService} would invert that one clean package edge into a cycle. Both classes belong to
 * the same target bounded context (N-MAILBOX), so the policy sits next to the forwarder until the
 * package cut of that context happens.
 *
 * <p>Policy order (do not reorder — the sender's status and the R-ACK depend on it):
 *
 * <ol>
 *   <li>explicit {@code oh_id} present and an {@code OutboundService} configured ⇒ authoritative
 *       path: validate {@code oh_id} / size / session tag / return path, try the local deposit,
 *       forward on {@code NOT_FOUND}, then emit the R-ACK and the opt-in status response;
 *   <li>otherwise the legacy garlic path: {@link #tryDepositToLocalOh} first, then the empty-{@code
 *       oh_id} frame validation (REDPANDAJ-2DR), then {@link GMParser#parse}.
 * </ol>
 */
@Slf4j
public final class MailboxDepositPolicy {

  private MailboxDepositPolicy() {}

  /**
   * Applies the deposit/forward/R-ACK policy to an already parsed {@code FlaschenpostPut}.
   *
   * @param serverContext context used for forwarding, R-ACKs and legacy garlic parsing
   * @param outboundService the local mailbox service, or {@code null} if this node runs without one
   *     (then only the legacy garlic path applies)
   * @param putMsg the parsed inbound message
   * @param peer the sending peer (receives the opt-in status response)
   */
  public static void handlePut(
      ServerContext serverContext,
      OutboundService outboundService,
      FlaschenpostPut putMsg,
      Peer peer) {
    ByteString contentBytes = putMsg.getContent();

    // MS01: Direct OH routing via explicit oh_id field.
    // MS02b: this path is authoritative — a packet with an explicit oh_id is deposited or
    // dropped (with an opt-in status response) and never falls through to the legacy garlic
    // parsing, which would misinterpret raw client payloads as GarlicMessages.
    ByteString ohIdBytes = putMsg.getOhId();
    if (!ohIdBytes.isEmpty() && outboundService != null) {
      // Validate OH id length before converting to a byte array to avoid large allocations
      if (ohIdBytes.size() != OhId.GARLIC_BYTES) {
        log.warn(
            "Received FlaschenpostPut with invalid oh_id length: {}, expected {}",
            ohIdBytes.size(),
            OhId.GARLIC_BYTES);
        respondToDeposit(outboundService, peer, putMsg, Status.BAD_REQUEST);
        return;
      }
      // The garlic length is inside OhId's general range, so this cannot fail.
      OhId ohId = OhId.fromBytes(ohIdBytes.toByteArray());
      // Pre-check the size limit before any deposit/forward decision: an oversized payload is
      // rejected by every host node anyway, so forwarding it (and answering OK) would only waste
      // hops and mislead the sender. Checked on the ByteString so an oversized payload is never
      // copied into a second array just to be rejected (cf. oh_id above).
      if (contentBytes.size() > OutboundMailboxStore.MAX_ITEM_BYTES) {
        respondToDeposit(outboundService, peer, putMsg, Status.BAD_REQUEST);
        return;
      }
      // MS05: a reverse-garlic session tag arrives here when the final garlic hop was not the
      // OH host and forwarded the tagged deliver (OhForwarder). Empty for direct deposits.
      // Validate the size on the ByteString before materializing the array (cf. oh_id above).
      ByteString sessionTagBytes = putMsg.getSessionTag();
      if (sessionTagBytes.size() != 0
          && sessionTagBytes.size() != OutboundService.SESSION_TAG_BYTES) {
        respondToDeposit(outboundService, peer, putMsg, Status.BAD_REQUEST);
        return;
      }
      byte[] sessionTag = sessionTagBytes.toByteArray();
      // MS06: a return-path block arrives here when a CMD_DELIVER_ACKED deliver was forwarded
      // by a non-host final garlic hop (OhForwarder, like the MS05 session tag). Structurally
      // invalid blocks reject the deposit like an invalid session tag.
      ByteString returnPathBytes = putMsg.getReturnPath();
      ReturnPath returnPath = null;
      if (!returnPathBytes.isEmpty()) {
        if (returnPathBytes.size() > ReturnPath.MAX_SERIALIZED_LEN) {
          respondToDeposit(outboundService, peer, putMsg, Status.BAD_REQUEST);
          return;
        }
        returnPath = ReturnPath.parseExact(returnPathBytes.toByteArray());
        if (returnPath == null) {
          respondToDeposit(outboundService, peer, putMsg, Status.BAD_REQUEST);
          return;
        }
      }
      byte[] content = contentBytes.toByteArray();
      OutboundService.DepositResult result =
          outboundService.depositMessage(ohId, content, sessionTag);
      if (result == OutboundService.DepositResult.NOT_FOUND) {
        // MS02b: not our OH — forward toward the host node (resolved via the DHT announce),
        // preserving the oh_id (and MS05 session tag / MS06 return path) on every hop.
        // Best-effort: OK means "accepted for forwarding".
        boolean accepted =
            OhForwarder.forward(
                serverContext,
                ohId,
                content,
                putMsg.getHopCount(),
                sessionTag,
                returnPathBytes.isEmpty() ? null : returnPathBytes.toByteArray());
        if (!accepted && returnPath != null) {
          // final station for this packet (hop limit) and the OH is unknown here — tell the
          // sender the handle could not be resolved instead of leaving it to the timeout
          RoutingAckSender.send(serverContext, returnPath, RoutingAckSender.STATUS_HANDLE_EXPIRED);
        }
        respondToDeposit(outboundService, peer, putMsg, accepted ? Status.OK : Status.NOT_FOUND);
        return;
      }
      if (result != OutboundService.DepositResult.DEPOSITED) {
        log.debug("FlaschenpostPut deposit not stored: {}", result);
      }
      if (returnPath != null) {
        // MS06: this node made the final deposit decision — send the R-ACK
        RoutingAckSender.send(serverContext, returnPath, RoutingAckSender.statusFor(result));
      }
      respondToDeposit(
          outboundService, peer, putMsg, OutboundService.depositResultToStatus(result));
      return;
    }

    // Legacy: Try to route via GarlicMessage destination header
    byte[] content = contentBytes.toByteArray();
    if (tryDepositToLocalOh(outboundService, content)) {
      return;
    }

    // REDPANDAJ-2DR hardening: an empty oh_id falls into legacy garlic parsing, which was
    // written to only ever see GarlicMessage/GMAck bytes. A raw E2E-encrypted client payload can
    // collide with a known GMType id (e.g. 0x04 == ACK) and must be rejected explicitly instead
    // of silently dropped by GMParser.parse's defensive fallback — otherwise the sender never
    // learns the deposit failed and keeps retrying blindly. Scoped to the empty-oh_id case only:
    // a non-empty oh_id that fell through here because outboundService is unset must keep its
    // pre-existing (unconditional) legacy behavior, not be rejected as if it were this case.
    if (ohIdBytes.isEmpty() && !GMParser.isValidFrame(serverContext, content)) {
      log.warn(
          "Rejecting FlaschenpostPut with empty oh_id whose content is not a valid GM frame,"
              + " length: {}",
          content.length);
      respondToDeposit(outboundService, peer, putMsg, Status.BAD_REQUEST);
      return;
    }

    GMParser.parse(serverContext, content);
  }

  /**
   * Sends the MS02b deposit status response, but only to directly connected light clients that
   * asked for it via {@code want_response}. Peers and legacy clients never receive command 158 —
   * their read loops would desync on an unknown command byte.
   */
  private static void respondToDeposit(
      OutboundService outboundService, Peer peer, FlaschenpostPut putMsg, Status status) {
    if (putMsg.getWantResponse() && peer.isLightClient() && outboundService != null) {
      outboundService.sendFlaschenpostPutResponse(peer, status);
    }
  }

  /**
   * Attempts to extract the destination KademliaId from a GarlicMessage-formatted payload and
   * deposit it into a locally registered Outbound Handle mailbox.
   *
   * <p><b>Scheduled for removal (MS02b domain-separation decision):</b> this legacy fallback treats
   * a 20-byte garlic <em>node</em> destination directly as an {@code oh_id}, so OH ids and node
   * KademliaIds share one undifferentiated namespace (a registered OH can shadow a node id). It
   * only exists because the explicit {@code oh_id} field was added after the first prototype; the
   * frontend has sent an explicit {@code oh_id} since Frontend-MS01. It is still reachable in
   * production, though: node-to-node garlic traffic is sent as a FlaschenpostPut without an {@code
   * oh_id} ({@link GMParser#sendFpToPeer}), so every forwarded garlic message runs through this
   * lookup. Once no legacy traffic remains, remove this method and the implicit shared-namespace
   * behavior — new code must never rely on it.
   *
   * @return true if the deposit targeted a locally registered OH — either stored, or rejected by
   *     the mailbox store's own limits (per-item size, item cap, byte quota). In both cases the
   *     packet is handled here and must not leak into the legacy forwarding pipeline. The
   *     empty-{@code oh_id} REDPANDAJ-2DR frame check runs afterwards, and only when this returns
   *     false.
   */
  private static boolean tryDepositToLocalOh(OutboundService outboundService, byte[] content) {
    if (outboundService == null) {
      return false;
    }
    // GarlicMessage format: [1 gmType][4 overallLen][20 destinationKademliaId]...
    int headerLen = 1 + 4 + KademliaId.ID_LENGTH_BYTES;
    if (content.length < headerLen) {
      return false;
    }
    try {
      byte[] destination = new byte[OhId.GARLIC_BYTES];
      System.arraycopy(content, 1 + 4, destination, 0, OhId.GARLIC_BYTES);
      OhId ohId = OhId.fromBytes(destination);
      // Anything other than NOT_FOUND targeted a locally registered OH: a rejected deposit
      // (quota/size) is handled here and must not leak into the legacy forwarding pipeline.
      return outboundService.depositMessage(ohId, content)
          != OutboundService.DepositResult.NOT_FOUND;
    } catch (RuntimeException e) {
      log.warn("Failed to extract destination or deposit message to local OH", e);
      return false;
    }
  }
}
