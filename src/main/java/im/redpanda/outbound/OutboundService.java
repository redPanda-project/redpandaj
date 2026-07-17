package im.redpanda.outbound;

import com.google.protobuf.ByteString;
import im.redpanda.core.Command;
import im.redpanda.core.Peer;
import im.redpanda.crypt.Utils;
import im.redpanda.outbound.OutboundAuth.AuthResult;
import im.redpanda.outbound.OutboundHandleStore.HandleRecord;
import im.redpanda.outbound.v1.AckFetchRequest;
import im.redpanda.outbound.v1.AckFetchResponse;
import im.redpanda.outbound.v1.FetchRequest;
import im.redpanda.outbound.v1.FetchResponse;
import im.redpanda.outbound.v1.FlaschenpostPutResponse;
import im.redpanda.outbound.v1.MailItem;
import im.redpanda.outbound.v1.Notify;
import im.redpanda.outbound.v1.RegisterOhRequest;
import im.redpanda.outbound.v1.RegisterOhResponse;
import im.redpanda.outbound.v1.RevokeOhRequest;
import im.redpanda.outbound.v1.RevokeOhResponse;
import im.redpanda.outbound.v1.Status;
import im.redpanda.outbound.v1.SubscribeRequest;
import im.redpanda.outbound.v1.SubscribeResponse;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

public class OutboundService {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(OutboundService.class);

  /** Result of a deposit attempt (MS02b: callers must distinguish rejection from "not local"). */
  public enum DepositResult {
    /** Stored in a locally registered OH mailbox. */
    DEPOSITED,
    /** The oh_id is not registered here (or expired) — candidate for forwarding. */
    NOT_FOUND,
    /** Mailbox full (item cap or byte quota); deposit rejected, nothing displaced. */
    QUOTA_EXCEEDED,
    /** Item exceeds the per-item size limit. */
    BAD_REQUEST
  }

  /**
   * Length in bytes of the per-item {@code message_id}: a raw RFC-4122 UUIDv4 ("16 bytes UUID raw",
   * see outbound.proto).
   */
  private static final int MESSAGE_ID_BYTES = 16;

  /** Required length of a non-empty MS05 reverse-garlic session tag. */
  public static final int SESSION_TAG_BYTES = 16;

  private static final byte[] EMPTY_SESSION_TAG = new byte[0];

  private final OutboundHandleStore handleStore;
  private final OutboundMailboxStore mailboxStore;
  private final OutboundAuth auth;
  private final SecureRandom secureRandom = new SecureRandom();

  /**
   * MS02b: invoked with the oh_id after every successful register, so the host node can announce
   * the OH → node mapping to the DHT promptly (wired up in App, no-op by default and in tests).
   */
  private volatile java.util.function.Consumer<byte[]> ohRegisteredListener = ohId -> {};

  // Configuration (could be in LocalSettings)
  private static final long MAX_TTL_MS = 7L * 24 * 60 * 60 * 1000; // 7 days
  private static final long MIN_TTL_MS = 10L * 60 * 1000; // 10 minutes

  // Input validation bounds (defense-in-depth against oversized fields)
  private static final int MIN_OH_ID_BYTES = 16;
  private static final int MAX_OH_ID_BYTES = 64;
  private static final int MIN_NONCE_BYTES = 8;
  private static final int MAX_NONCE_BYTES = 64;

  // MS02b: register rate limit per connection (handle-store exhaustion defense).
  // Counted per RegisterOhRequest before signature verification, sliding window.
  static final int REGISTER_LIMIT_PER_WINDOW = 5;
  static final long REGISTER_WINDOW_MS = 60_000;

  /**
   * Register timestamps per connection. WeakHashMap so entries vanish with the Peer object after
   * disconnect; all access goes through the synchronized wrapper plus per-deque synchronization.
   */
  private final Map<Peer, Deque<Long>> registerHistory =
      Collections.synchronizedMap(new WeakHashMap<>());

  /**
   * Connection-Notify (T38) subscription registry: oh_id (hex) → the peer connection that proved
   * ownership via a signed {@link SubscribeRequest}. The value is a {@link WeakReference} so a
   * disconnected peer's binding vanishes with the {@code Peer} object (same self-cleaning rationale
   * as {@link #registerHistory}) — no persisted subscription state, no dead-peer leak. The deposit
   * side additionally prunes any binding whose peer is gone or no longer connected, so a Notify is
   * never sent to a disconnected client. Re-subscribe is idempotent (overwrites the same key);
   * multiple OHs may map to the same peer, and an oh_id is bound to at most one connection
   * (last-writer-wins — the OH owner controls this via its signing key).
   */
  private final ConcurrentHashMap<String, WeakReference<Peer>> subscriptions =
      new ConcurrentHashMap<>();

  public OutboundService(OutboundHandleStore handleStore, OutboundMailboxStore mailboxStore) {
    this.handleStore = handleStore;
    this.mailboxStore = mailboxStore;
    this.auth = new OutboundAuth();
  }

  public void handleRegister(Peer peer, RegisterOhRequest req) {
    long now = System.currentTimeMillis();

    byte[] ohId = req.getOhId().toByteArray();
    byte[] nonce = req.getNonce().toByteArray();
    long timestamp = req.getTimestampMs();
    long requestedExpires = req.getRequestedExpiresAt();

    // Input validation: reject oversized or empty fields
    if (outOfRange(ohId.length, MIN_OH_ID_BYTES, MAX_OH_ID_BYTES)
        || outOfRange(nonce.length, MIN_NONCE_BYTES, MAX_NONCE_BYTES)) {
      sendRegisterResponse(peer, Status.BAD_REQUEST, 0);
      return;
    }

    // MS02b: rate-limit registers per connection before the (expensive) signature check
    if (registerRateLimited(peer, now)) {
      sendRegisterResponse(peer, Status.RATE_LIMIT, 0);
      return;
    }

    // Reconstruct signing bytes (unversioned body — OutboundAuth applies the MS03 version byte)
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

    // MS02b: make the fresh handle resolvable via the DHT announce. Best-effort — a failing
    // announce hook must never break the register flow (the handle is already stored).
    try {
      ohRegisteredListener.accept(ohId);
    } catch (RuntimeException e) {
      logger.warn("OH announce hook failed after register", e);
    }

    // 3. Response
    sendRegisterResponse(peer, Status.OK, validExpiresAt);
  }

  /** Sets the MS02b post-register hook (DHT announce trigger). */
  public void setOhRegisteredListener(java.util.function.Consumer<byte[]> listener) {
    this.ohRegisteredListener = listener != null ? listener : ohId -> {};
  }

  public void handleFetch(Peer peer, FetchRequest req) {
    long now = System.currentTimeMillis();
    byte[] ohId = req.getOhId().toByteArray();
    byte[] nonce = req.getNonce().toByteArray();
    long timestamp = req.getTimestampMs();

    // Input validation
    if (outOfRange(ohId.length, MIN_OH_ID_BYTES, MAX_OH_ID_BYTES)
        || outOfRange(nonce.length, MIN_NONCE_BYTES, MAX_NONCE_BYTES)) {
      sendFetchResponse(peer, Status.BAD_REQUEST, 0, List.of(), false);
      return;
    }

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

    // Input validation
    if (outOfRange(ohId.length, MIN_OH_ID_BYTES, MAX_OH_ID_BYTES)
        || outOfRange(nonce.length, MIN_NONCE_BYTES, MAX_NONCE_BYTES)) {
      sendRevokeResponse(peer, Status.BAD_REQUEST);
      return;
    }

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
    mailboxStore.deleteAll(ohId);

    sendRevokeResponse(peer, Status.OK);
  }

  /**
   * Connection-Notify (T38): handles a Subscribe request. Verifies OH ownership exactly like {@link
   * #handleFetch} (Ed25519 signature against the stored OH key, same timestamp/replay handling),
   * then binds {@code oh_id → this peer connection} so future deposits into that mailbox trigger a
   * one-way {@link Notify}. The binding is in-memory only and ends when the peer disconnects; a
   * repeated subscribe is idempotent.
   *
   * <p>Signing bytes: {@code [CMD_BYTE | oh_id | timestamp(8) | nonce]} — prefixed with the MS03
   * version byte by {@link OutboundAuth} for Ed25519 verification (same layout as Revoke).
   */
  public void handleSubscribe(Peer peer, SubscribeRequest req) {
    long now = System.currentTimeMillis();
    byte[] ohId = req.getOhId().toByteArray();
    byte[] nonce = req.getNonce().toByteArray();
    long timestamp = req.getTimestampMs();

    // Input validation
    if (outOfRange(ohId.length, MIN_OH_ID_BYTES, MAX_OH_ID_BYTES)
        || outOfRange(nonce.length, MIN_NONCE_BYTES, MAX_NONCE_BYTES)) {
      sendSubscribeResponse(peer, Status.BAD_REQUEST);
      return;
    }

    HandleRecord handle = handleStore.get(ohId);
    if (handle == null || handle.getExpiresAtMs() < now) {
      sendSubscribeResponse(peer, Status.NOT_FOUND);
      return;
    }

    ByteBuffer signBuf = ByteBuffer.allocate(1 + ohId.length + 8 + nonce.length);
    signBuf.put(Command.OUTBOUND_SUBSCRIBE_REQ);
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
      sendSubscribeResponse(peer, mapAuthToStatus(result));
      return;
    }

    // Bind oh_id → this connection (idempotent overwrite; last-writer-wins across connections).
    subscriptions.put(Utils.bytesToHexString(ohId), new WeakReference<>(peer));
    sendSubscribeResponse(peer, Status.OK);
  }

  /**
   * Connection-Notify (T38): fire-and-forget one-way {@link Notify} to the connection subscribed
   * for {@code ohId}, if any. Called after every successful deposit (all deposit paths funnel
   * through {@link #depositMessage}). Strictly opt-in: only a peer that sent a valid {@link
   * SubscribeRequest} is in the registry, so existing clients never receive command 161. Any send
   * failure is swallowed — a deposit must never be affected by notification delivery. Prunes the
   * binding if its peer has been garbage-collected or is no longer connected.
   */
  private void notifySubscriber(byte[] ohId) {
    try {
      String key = Utils.bytesToHexString(ohId);
      WeakReference<Peer> ref = subscriptions.get(key);
      if (ref == null) {
        return;
      }
      Peer peer = ref.get();
      if (peer == null || !peer.isConnected()) {
        subscriptions.remove(key);
        return;
      }
      Notify notify = Notify.newBuilder().setOhId(ByteString.copyFrom(ohId)).build();
      writeResponse(peer, Command.OUTBOUND_NOTIFY, notify.toByteArray());
    } catch (RuntimeException e) {
      logger.warn("Connection-Notify: failed to notify subscriber", e);
    }
  }

  /**
   * Handles an AckFetch request: verifies the signature, deletes all mailbox items with sequence_id
   * &lt;= acked_sequence_id, and responds with OK.
   *
   * <p>Signing bytes: {@code [CMD_BYTE | oh_id | acked_sequence_id(8) | timestamp(8) | nonce]} —
   * prefixed with the MS03 version byte by {@link OutboundAuth} for Ed25519 verification
   */
  public void handleAckFetch(Peer peer, AckFetchRequest req) {
    long now = System.currentTimeMillis();
    byte[] ohId = req.getOhId().toByteArray();
    byte[] nonce = req.getNonce().toByteArray();
    long timestamp = req.getTimestampMs();

    // Input validation
    if (outOfRange(ohId.length, MIN_OH_ID_BYTES, MAX_OH_ID_BYTES)
        || outOfRange(nonce.length, MIN_NONCE_BYTES, MAX_NONCE_BYTES)) {
      sendAckFetchResponse(peer, Status.BAD_REQUEST);
      return;
    }

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
   * <p>A fresh {@code message_id} (raw RFC-4122 UUIDv4, generated from cryptographically random
   * bytes) is assigned here, at deposit time, before the item is serialized into the mailbox store.
   * This guarantees the id is persisted with the item and stays stable across re-fetches of the
   * same un-acked item. The frontend uses hex(message_id) as its deduplication key, so an
   * unset/empty id would collapse every message to the same key and cause the receiver to drop
   * everything after the first.
   *
   * @param ohId the outbound handle identifier
   * @param payload the raw message payload to deposit
   * @return {@link DepositResult#DEPOSITED} on success, {@link DepositResult#NOT_FOUND} if the OH
   *     is not registered here or expired, or the MS02b rejection reason
   */
  public DepositResult depositMessage(byte[] ohId, byte[] payload) {
    return depositMessage(ohId, payload, EMPTY_SESSION_TAG);
  }

  /**
   * Deposits a message with an MS05 reverse-garlic session tag (see {@link #depositMessage(byte[],
   * byte[])} for the deposit semantics). The tag is stored on the {@code MailItem} and returned to
   * the fetching client, which uses it to correlate the reply with a conversation.
   *
   * @param sessionTag 16-byte session tag, or empty/{@code null} for untagged messages; any other
   *     length is rejected with {@link DepositResult#BAD_REQUEST}
   */
  public DepositResult depositMessage(byte[] ohId, byte[] payload, byte[] sessionTag) {
    if (sessionTag == null) {
      sessionTag = EMPTY_SESSION_TAG;
    }
    if (sessionTag.length != 0 && sessionTag.length != SESSION_TAG_BYTES) {
      return DepositResult.BAD_REQUEST;
    }
    HandleRecord handle = handleStore.get(ohId);
    if (handle == null) {
      return DepositResult.NOT_FOUND;
    }
    long now = System.currentTimeMillis();
    if (handle.getExpiresAtMs() < now) {
      return DepositResult.NOT_FOUND;
    }
    byte[] messageId = newMessageId();
    MailItem item =
        MailItem.newBuilder()
            .setMessageId(ByteString.copyFrom(messageId))
            .setReceivedAtMs(now)
            .setPayload(ByteString.copyFrom(payload))
            .setSessionTag(ByteString.copyFrom(sessionTag))
            .build();
    DepositResult result =
        switch (mailboxStore.addMessage(ohId, item)) {
          case ADDED -> DepositResult.DEPOSITED;
          case REJECTED_ITEM_TOO_LARGE -> DepositResult.BAD_REQUEST;
          case REJECTED_MAILBOX_FULL, REJECTED_BYTE_QUOTA -> DepositResult.QUOTA_EXCEEDED;
        };
    // Connection-Notify (T38): signal the subscribed connection (if any) that new mail arrived.
    // Only on an actual deposit — a rejected deposit stores nothing, so there is nothing to fetch.
    if (result == DepositResult.DEPOSITED) {
      notifySubscriber(ohId);
    }
    return result;
  }

  /**
   * Sends a {@link FlaschenpostPutResponse} for a deposit attempt. Only called for directly
   * connected light clients that set {@code want_response} (full nodes and legacy clients must
   * never receive command 158 — unknown commands desync their read loop).
   */
  public void sendFlaschenpostPutResponse(Peer peer, Status status) {
    FlaschenpostPutResponse res =
        FlaschenpostPutResponse.newBuilder()
            .setStatus(status)
            .setServerTimeMs(System.currentTimeMillis())
            .build();
    writeResponse(peer, Command.FLASCHENPOST_PUT_RES, res.toByteArray());
  }

  /** Maps a {@link DepositResult} to the wire status for FlaschenpostPutResponse. */
  public static Status depositResultToStatus(DepositResult result) {
    return switch (result) {
      case DEPOSITED -> Status.OK;
      case NOT_FOUND -> Status.NOT_FOUND;
      case QUOTA_EXCEEDED -> Status.QUOTA_EXCEEDED;
      case BAD_REQUEST -> Status.BAD_REQUEST;
    };
  }

  /**
   * Sliding-window register rate limit per connection: returns true (and rejects) if more than
   * {@link #REGISTER_LIMIT_PER_WINDOW} registers arrived within {@link #REGISTER_WINDOW_MS}.
   */
  private boolean registerRateLimited(Peer peer, long now) {
    Deque<Long> history;
    // computeIfAbsent is not covered by the synchronizedMap wrapper — do get-or-create atomically
    synchronized (registerHistory) {
      history = registerHistory.computeIfAbsent(peer, p -> new ArrayDeque<>());
    }
    synchronized (history) {
      while (!history.isEmpty() && now - history.peekFirst() > REGISTER_WINDOW_MS) {
        history.pollFirst();
      }
      if (history.size() >= REGISTER_LIMIT_PER_WINDOW) {
        return true;
      }
      history.addLast(now);
      return false;
    }
  }

  // --- Helpers ---

  /**
   * Generates a raw RFC-4122 UUIDv4 (random bytes with version nibble 4 and IETF variant bits) so
   * the wire value matches the "16 bytes UUID raw" contract documented in outbound.proto.
   */
  private byte[] newMessageId() {
    byte[] id = new byte[MESSAGE_ID_BYTES];
    secureRandom.nextBytes(id);
    id[6] = (byte) ((id[6] & 0x0F) | 0x40); // version 4
    id[8] = (byte) ((id[8] & 0x3F) | 0x80); // IETF variant
    return id;
  }

  /** Returns true if the field length is outside the allowed range (inclusive). */
  private static boolean outOfRange(int length, int min, int max) {
    return length < min || length > max;
  }

  private long clampExpiresAt(long now, long requested) {
    long max = now + MAX_TTL_MS;
    long min = now + MIN_TTL_MS;
    if (requested > max) return max;
    if (requested < min) return min;
    return requested;
  }

  private static Status mapAuthToStatus(AuthResult result) {
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

  private void sendSubscribeResponse(Peer peer, Status status) {
    SubscribeResponse res =
        SubscribeResponse.newBuilder()
            .setStatus(status)
            .setServerTimeMs(System.currentTimeMillis())
            .build();
    writeResponse(peer, Command.OUTBOUND_SUBSCRIBE_RES, res.toByteArray());
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
