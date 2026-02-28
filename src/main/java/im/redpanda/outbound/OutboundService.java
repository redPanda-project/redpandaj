package im.redpanda.outbound;

import com.google.protobuf.ByteString;
import im.redpanda.core.Command;
import im.redpanda.core.Peer;
import im.redpanda.outbound.OutboundAuth.AuthResult;
import im.redpanda.outbound.OutboundHandleStore.HandleRecord;
import im.redpanda.outbound.v1.AckFetchRequest;
import im.redpanda.outbound.v1.AckFetchResponse;
import im.redpanda.outbound.v1.FetchRequest;
import im.redpanda.outbound.v1.FetchResponse;
import im.redpanda.outbound.v1.MailItem;
import im.redpanda.outbound.v1.RegisterOhRequest;
import im.redpanda.outbound.v1.RegisterOhResponse;
import im.redpanda.outbound.v1.RevokeOhRequest;
import im.redpanda.outbound.v1.RevokeOhResponse;
import im.redpanda.outbound.v1.Status;
import java.nio.ByteBuffer;
import java.util.List;

public class OutboundService {

  private final OutboundHandleStore handleStore;
  private final OutboundMailboxStore mailboxStore;
  private final OutboundAuth auth;

  // Configuration (could be in LocalSettings)
  private static final long MAX_TTL_MS = 7L * 24 * 60 * 60 * 1000; // 7 days
  private static final long MIN_TTL_MS = 10L * 60 * 1000; // 10 minutes

  public OutboundService(OutboundHandleStore handleStore, OutboundMailboxStore mailboxStore) {
    this.handleStore = handleStore;
    this.mailboxStore = mailboxStore;
    this.auth = new OutboundAuth();
  }

  public void handleRegister(Peer peer, RegisterOhRequest req) {
    long now = System.currentTimeMillis();

    // 1. Auth: Verify signature over oh_id, requested_expires_at, timestamp_ms, and
    // nonce.

    byte[] ohId = req.getOhId().toByteArray();
    byte[] nonce = req.getNonce().toByteArray();
    long timestamp = req.getTimestampMs();
    long requestedExpires = req.getRequestedExpiresAt();

    // Reconstruct signing bytes
    ByteBuffer signBuf = ByteBuffer.allocate(1 + ohId.length + 8 + 8 + nonce.length);
    signBuf.put(Command.OUTBOUND_REGISTER_OH_REQ);
    signBuf.put(ohId);
    signBuf.putLong(requestedExpires);
    signBuf.putLong(timestamp);
    signBuf.put(nonce);

    AuthResult result =
        auth.verify(
            req.getOhAuthPublicKey().toByteArray(),
            signBuf.array(),
            req.getSignature().toByteArray(),
            timestamp,
            ohId,
            nonce);

    if (result != AuthResult.OK) {
      sendRegisterResponse(peer, mapAuthToStatus(result), 0);
      return;
    }

    // 2. Logic
    long validExpiresAt = clampExpiresAt(now, requestedExpires);
    HandleRecord handleRecord =
        new HandleRecord(req.getOhAuthPublicKey().toByteArray(), now, validExpiresAt);

    handleStore.put(ohId, handleRecord);

    // 3. Response
    sendRegisterResponse(peer, Status.OK, validExpiresAt);
  }

  public void handleFetch(Peer peer, FetchRequest req) {
    long now = System.currentTimeMillis();
    byte[] ohId = req.getOhId().toByteArray();
    byte[] nonce = req.getNonce().toByteArray();
    long timestamp = req.getTimestampMs();

    // Auth checks
    HandleRecord handle = handleStore.get(ohId);
    if (handle == null) {
      sendFetchResponse(peer, Status.NOT_FOUND, 0, List.of(), false);
      return;
    }

    if (handle.getExpiresAtMs() < now) {
      // Should have been cleaned up, but explicitly reject
      sendFetchResponse(peer, Status.NOT_FOUND, 0, List.of(), false);
      return;
    }

    // 2. Client signs: commandId + ohId + timestamp + nonce + limit + cursor
    ByteBuffer signBuf = ByteBuffer.allocate(1 + ohId.length + 8 + nonce.length + 4 + 8);
    signBuf.put(Command.OUTBOUND_FETCH_REQ);
    signBuf.put(ohId);
    signBuf.putLong(timestamp);
    signBuf.put(nonce);
    // limit is int32, cursor is int64 (used as afterSequence)
    signBuf.putInt(req.getLimit());
    signBuf.putLong(req.getCursor());

    AuthResult result =
        auth.verify(
            handle.getOhAuthPublicKey(), // Verify against the stored public key for this OH
            signBuf.array(),
            req.getSignature().toByteArray(),
            timestamp,
            ohId,
            nonce);

    if (result != AuthResult.OK) {
      sendFetchResponse(peer, mapAuthToStatus(result), 0, List.of(), false);
      return;
    }

    // Fetch logic — cursor is now afterSequence
    List<MailItem> items = mailboxStore.fetchMessages(ohId, req.getLimit(), req.getCursor());
    // next_cursor = highest sequence_id returned, or current cursor if no items
    long nextCursor =
        items.isEmpty() ? req.getCursor() : items.get(items.size() - 1).getSequenceId();
    boolean overflow = mailboxStore.checkAndClearOverflow(ohId);
    sendFetchResponse(peer, Status.OK, nextCursor, items, overflow);
  }

  public void handleRevoke(Peer peer, RevokeOhRequest req) {
    byte[] ohId = req.getOhId().toByteArray();
    byte[] nonce = req.getNonce().toByteArray();
    long timestamp = req.getTimestampMs();

    HandleRecord handle = handleStore.get(ohId);
    if (handle == null) {
      sendRevokeResponse(peer, Status.NOT_FOUND);
      return;
    }

    ByteBuffer signBuf = ByteBuffer.allocate(1 + ohId.length + 8 + nonce.length);
    signBuf.put(Command.OUTBOUND_REVOKE_OH_REQ);
    signBuf.put(ohId);
    signBuf.putLong(timestamp);
    signBuf.put(nonce);

    AuthResult result =
        auth.verify(
            handle.getOhAuthPublicKey(),
            signBuf.array(),
            req.getSignature().toByteArray(),
            timestamp,
            ohId,
            nonce);

    if (result != AuthResult.OK) {
      sendRevokeResponse(peer, mapAuthToStatus(result));
      return;
    }

    handleStore.remove(ohId);

    sendRevokeResponse(peer, Status.OK);
  }

  /**
   * Handles an AckFetch request: verifies the signature, deletes all mailbox items with sequence_id
   * &lt;= acked_sequence_id, and responds with OK.
   *
   * <p>Signing bytes: {@code [CMD_BYTE | oh_id | acked_sequence_id(8) | timestamp(8) | nonce]}
   */
  public void handleAckFetch(Peer peer, AckFetchRequest req) {
    long now = System.currentTimeMillis();
    byte[] ohId = req.getOhId().toByteArray();
    byte[] nonce = req.getNonce().toByteArray();
    long timestamp = req.getTimestampMs();

    HandleRecord handle = handleStore.get(ohId);
    if (handle == null) {
      sendAckFetchResponse(peer, Status.NOT_FOUND);
      return;
    }

    if (handle.getExpiresAtMs() < now) {
      sendAckFetchResponse(peer, Status.NOT_FOUND);
      return;
    }

    // Signing bytes: CMD_BYTE | oh_id | acked_sequence_id(8) | timestamp(8) | nonce
    long ackedSeqId = req.getAckedSequenceId();
    ByteBuffer signBuf = ByteBuffer.allocate(1 + ohId.length + 8 + 8 + nonce.length);
    signBuf.put(Command.OUTBOUND_ACK_FETCH_REQ);
    signBuf.put(ohId);
    signBuf.putLong(ackedSeqId);
    signBuf.putLong(timestamp);
    signBuf.put(nonce);

    AuthResult result =
        auth.verify(
            handle.getOhAuthPublicKey(),
            signBuf.array(),
            req.getSignature().toByteArray(),
            timestamp,
            ohId,
            nonce);

    if (result != AuthResult.OK) {
      sendAckFetchResponse(peer, mapAuthToStatus(result));
      return;
    }

    mailboxStore.deleteUpTo(ohId, ackedSeqId);
    sendAckFetchResponse(peer, Status.OK);
  }

  /**
   * Deposits a message into the mailbox for the given OH, if it exists and is not expired.
   *
   * @param ohId the outbound handle identifier
   * @param payload the raw message payload to deposit
   * @return true if the message was deposited, false if the OH was not found or expired
   */
  public boolean depositMessage(byte[] ohId, byte[] payload) {
    HandleRecord handle = handleStore.get(ohId);
    if (handle == null) {
      return false;
    }
    if (handle.getExpiresAtMs() < System.currentTimeMillis()) {
      return false;
    }
    MailItem item =
        MailItem.newBuilder()
            .setReceivedAtMs(System.currentTimeMillis())
            .setPayload(ByteString.copyFrom(payload))
            .build();
    mailboxStore.addMessage(ohId, item);
    return true;
  }

  // --- Helpers ---

  private long clampExpiresAt(long now, long requested) {
    long max = now + MAX_TTL_MS;
    long min = now + MIN_TTL_MS;
    if (requested > max) return max;
    if (requested < min) return min;
    return requested;
  }

  private Status mapAuthToStatus(AuthResult result) {
    return switch (result) {
      case OK -> Status.OK;
      case INVALID_SIGNATURE -> Status.INVALID_SIGNATURE;
      case INVALID_TIMESTAMP -> Status.INVALID_TIMESTAMP;
      case REPLAY -> Status.REPLAY;
      default -> Status.STATUS_UNSPECIFIED;
    };
  }

  private void sendRegisterResponse(Peer peer, Status status, long expiresAt) {
    RegisterOhResponse res =
        RegisterOhResponse.newBuilder()
            .setStatus(status)
            .setServerTimeMs(System.currentTimeMillis())
            .setExpiresAtMs(expiresAt)
            .build();

    writeResponse(peer, Command.OUTBOUND_REGISTER_OH_RES, res.toByteArray());
  }

  private void sendFetchResponse(
      Peer peer, Status status, long nextCursor, List<MailItem> items, boolean overflow) {
    FetchResponse res =
        FetchResponse.newBuilder()
            .setStatus(status)
            .setServerTimeMs(System.currentTimeMillis())
            .setNextCursor(nextCursor)
            .addAllItems(items)
            .setMailboxOverflow(overflow)
            .build();
    writeResponse(peer, Command.OUTBOUND_FETCH_RES, res.toByteArray());
  }

  private void sendRevokeResponse(Peer peer, Status status) {
    RevokeOhResponse res =
        RevokeOhResponse.newBuilder()
            .setStatus(status)
            .setServerTimeMs(System.currentTimeMillis())
            .build();
    writeResponse(peer, Command.OUTBOUND_REVOKE_OH_RES, res.toByteArray());
  }

  private void sendAckFetchResponse(Peer peer, Status status) {
    AckFetchResponse res =
        AckFetchResponse.newBuilder()
            .setStatus(status)
            .setServerTimeMs(System.currentTimeMillis())
            .build();
    writeResponse(peer, Command.OUTBOUND_ACK_FETCH_RES, res.toByteArray());
  }

  // Helper to write [cmd][len][payload]
  private void writeResponse(Peer peer, byte command, byte[] payload) {
    peer.getWriteBufferLock().lock();
    try {
      peer.writeBuffer.put(command);
      peer.writeBuffer.putInt(payload.length);
      peer.writeBuffer.put(payload);
      peer.setWriteBufferFilled();
    } finally {
      peer.getWriteBufferLock().unlock();
    }
  }
}
