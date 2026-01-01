package im.redpanda.outbound;

import im.redpanda.core.Command;
import im.redpanda.core.Peer;
import im.redpanda.outbound.OutboundAuth.AuthResult;
import im.redpanda.outbound.OutboundHandleStore.HandleRecord;
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

    // 1. Auth
    // Bytes to sign: oh_id || requested_expires_at || timestamp_ms || nonce
    // NOTE: The user plan suggested signing: commandId || oh_id ...
    // But the request object doesn't have commandId in it directly.
    // For PoC, let's sign what we have in the request:
    // oh_id + requested_expires_at + timestamp_ms + nonce
    // To match the user's plan STRICTLY we would need to reconstructing the signing
    // buffer carefully.
    // "Signiert wird über deterministische Bytes"

    byte[] ohId = req.getOhId().toByteArray();
    byte[] nonce = req.getNonce().toByteArray();
    long timestamp = req.getTimestampMs();
    long requestedExpires = req.getRequestedExpiresAt();

    // Reconstruct signing bytes
    ByteBuffer signBuf = ByteBuffer.allocate(1 + ohId.length + 8 + 8 + nonce.length);
    signBuf.put(Command.OUTBOUND_REGISTER_OH_REQ); // Command ID included as per plan
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
    HandleRecord record =
        new HandleRecord(req.getOhAuthPublicKey().toByteArray(), now, validExpiresAt);

    handleStore.put(ohId, record);

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
      sendFetchResponse(peer, Status.NOT_FOUND, 0, List.of());
      return;
    }

    if (handle.expiresAtMs < now) {
      // Should have been cleaned up, but explicitly reject
      sendFetchResponse(peer, Status.NOT_FOUND, 0, List.of());
      return;
    }

    // Signing bytes: commandId || oh_id || timestamp || nonce || limit || cursor...
    // We need to match the client's signing logic.
    ByteBuffer signBuf = ByteBuffer.allocate(1 + ohId.length + 8 + 8 + nonce.length + 4 + 8);
    signBuf.put(Command.OUTBOUND_FETCH_REQ);
    signBuf.put(ohId);
    signBuf.putLong(timestamp);
    signBuf.put(nonce);
    // limit is int32, cursor is int64
    signBuf.putInt(req.getLimit());
    signBuf.putLong(req.getCursor()); // optional field, usually 0 if PoC

    AuthResult result =
        auth.verify(
            handle.ohAuthPublicKey, // Verify against the stored public key for this OH
            signBuf.array(),
            req.getSignature().toByteArray(),
            timestamp,
            ohId,
            nonce);

    if (result != AuthResult.OK) {
      sendFetchResponse(peer, mapAuthToStatus(result), 0, List.of());
      return;
    }

    // Fetch logic
    List<MailItem> items = mailboxStore.fetchMessages(ohId, req.getLimit(), req.getCursor());
    long nextCursor = req.getCursor() + items.size();
    sendFetchResponse(peer, Status.OK, nextCursor, items);
  }

  public void handleRevoke(Peer peer, RevokeOhRequest req) {
    byte[] ohId = req.getOhId().toByteArray();
    byte[] nonce = req.getNonce().toByteArray();
    long timestamp = req.getTimestampMs();

    HandleRecord handle = handleStore.get(ohId);
    if (handle == null) { // Already gone is OK or Not Found?
      // If we can't find it, we can't verify signature because we don't have the
      // pubkey.
      // So we must return Not Found.
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
            handle.ohAuthPublicKey,
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

  // --- Helpers ---

  private long clampExpiresAt(long now, long requested) {
    long max = now + MAX_TTL_MS;
    long min = now + MIN_TTL_MS;
    if (requested > max) return max;
    if (requested < min) return min;
    return requested;
  }

  private Status mapAuthToStatus(AuthResult result) {
    switch (result) {
      case OK:
        return Status.OK;
      case INVALID_SIGNATURE:
        return Status.INVALID_SIGNATURE;
      case INVALID_TIMESTAMP:
        return Status.INVALID_TIMESTAMP;
      case REPLAY:
        return Status.REPLAY;
      default:
        return Status.STATUS_UNSPECIFIED;
    }
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

  private void sendFetchResponse(Peer peer, Status status, long nextCursor, List<MailItem> items) {
    FetchResponse res =
        FetchResponse.newBuilder()
            .setStatus(status)
            .setServerTimeMs(System.currentTimeMillis())
            .setNextCursor(nextCursor)
            .addAllItems(items)
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
