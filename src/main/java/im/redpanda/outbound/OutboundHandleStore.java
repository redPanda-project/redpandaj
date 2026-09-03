package im.redpanda.outbound;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import im.redpanda.core.StateFormat;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handle registry facade of the mailbox context: oh_id → {@link HandleRecord} (the lease of a
 * client on a mailbox of this node).
 *
 * <p>T109: this class no longer owns a database. It is one of the two facades over the single
 * {@link OutboundStore} database; all writes go through the owner's transaction so they commit
 * together with the mailbox writes of the same operation. Removing a handle is deliberately
 * <b>not</b> part of this API — a handle and its mailbox are removed together via {@link
 * OutboundStore#removeHandle(OhId)} / {@link OutboundStore#cleanupExpiredHandles(long)}.
 *
 * <p>T113: the map key is {@link OhId#toHex()}. The hex encoding is the persisted key format, so
 * the conversion lives in {@link OhId} and nowhere else.
 */
public class OutboundHandleStore {

  private final OutboundStore owner;
  private final Map<String, HandleRecord> handles;

  /**
   * The lease a client holds on a mailbox of this node.
   *
   * <p>T117 (TD111): persisted as explicit JSON, not with MapDB's {@code Serializer.JAVA}. That
   * serializer pinned this class's fully qualified name into {@code data/outbound_*.mapdb}, so the
   * package moves of T118 would have left every node unable to read its own handle registry (DDD
   * review §5). The map holding these records is named {@code handlesV2}, which is where the format
   * version lives — the records themselves carry no header.
   */
  public static class HandleRecord {
    private final byte[] ohAuthPublicKey;
    private final long createdAtMs;
    private final long expiresAtMs;

    public HandleRecord(byte[] ohAuthPublicKey, long createdAtMs, long expiresAtMs) {
      this.ohAuthPublicKey = ohAuthPublicKey;
      this.createdAtMs = createdAtMs;
      this.expiresAtMs = expiresAtMs;
    }

    public byte[] getOhAuthPublicKey() {
      return ohAuthPublicKey;
    }

    public long getCreatedAtMs() {
      return createdAtMs;
    }

    public long getExpiresAtMs() {
      return expiresAtMs;
    }

    byte[] toJsonBytes() {
      JsonObject json = new JsonObject();
      json.addProperty("ohAuthPublicKey", StateFormat.base64(ohAuthPublicKey));
      json.addProperty("createdAtMs", createdAtMs);
      json.addProperty("expiresAtMs", expiresAtMs);
      return new Gson().toJson(json).getBytes(StandardCharsets.UTF_8);
    }

    static HandleRecord fromJsonBytes(byte[] bytes) throws IOException {
      final JsonObject json;
      try {
        json = JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject();
      } catch (RuntimeException e) {
        throw new IOException("handle record is not a JSON object", e);
      }
      // Every field is required. Defaulting them would turn a corrupt record into a lease that
      // looks valid: a null auth key NPEs in OutboundAuth.verifySignature, and a defaulted
      // expiresAtMs silently shortens or extends the lease.
      byte[] authKey = StateFormat.optBase64(json, "ohAuthPublicKey");
      if (authKey == null) {
        throw new IOException("handle record without an ohAuthPublicKey");
      }
      if (!json.has("createdAtMs") || !json.has("expiresAtMs")) {
        throw new IOException("handle record without its timestamps");
      }
      return new HandleRecord(
          authKey,
          StateFormat.optLong(json, "createdAtMs", 0L),
          StateFormat.optLong(json, "expiresAtMs", 0L));
    }
  }

  OutboundHandleStore(OutboundStore owner, Map<String, HandleRecord> handles) {
    this.owner = owner;
    this.handles = handles;
  }

  /** Registers (or renews — idempotent overwrite) the handle for an oh_id. */
  public void put(OhId ohId, HandleRecord record) {
    String handleKey = ohId.toHex();
    owner.tx(
        () -> {
          handles.put(handleKey, record);
          owner.markDirty();
        });
  }

  public HandleRecord get(OhId ohId) {
    String handleKey = ohId.toHex();
    return owner.read(() -> handles.get(handleKey));
  }

  /**
   * Returns the oh_ids of all non-expired handles (MS02b: used by the periodic DHT announce job).
   */
  public List<OhId> listActiveOhIds(long now) {
    return owner.read(
        () -> {
          List<OhId> result = new ArrayList<>();
          for (Map.Entry<String, HandleRecord> entry : handles.entrySet()) {
            HandleRecord record = entry.getValue();
            if (record != null && record.getExpiresAtMs() >= now) {
              result.add(OhId.fromHex(entry.getKey()));
            }
          }
          return result;
        });
  }

  /** All handles that expired before {@code now} — snapshot, safe to remove while iterating it. */
  List<OhId> expiredBefore(long now) {
    return owner.read(
        () -> {
          List<OhId> result = new ArrayList<>();
          for (Map.Entry<String, HandleRecord> entry : handles.entrySet()) {
            HandleRecord record = entry.getValue();
            if (record != null && record.getExpiresAtMs() < now) {
              result.add(OhId.fromHex(entry.getKey()));
            }
          }
          return result;
        });
  }

  /**
   * Removes the handle only. Package-private on purpose: the mailbox of that handle must be deleted
   * in the same transaction, which is what {@link OutboundStore#removeHandle(OhId)} does.
   */
  void remove(OhId ohId) {
    String handleKey = ohId.toHex();
    owner.tx(
        () -> {
          if (handles.remove(handleKey) != null) {
            owner.markDirty();
          }
        });
  }
}
