package im.redpanda.outbound;

import java.io.Serializable;
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

  public static class HandleRecord implements Serializable {
    private static final long serialVersionUID = 1L;
    private byte[] ohAuthPublicKey;
    private long createdAtMs;
    private long expiresAtMs;

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
