package im.redpanda.outbound;

import im.redpanda.crypt.Utils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bouncycastle.util.encoders.Hex;

/**
 * Handle registry facade of the mailbox context: oh_id → {@link HandleRecord} (the lease of a
 * client on a mailbox of this node).
 *
 * <p>T109: this class no longer owns a database. It is one of the two facades over the single
 * {@link OutboundStore} database; all writes go through the owner's transaction so they commit
 * together with the mailbox writes of the same operation. Removing a handle is deliberately
 * <b>not</b> part of this API — a handle and its mailbox are removed together via {@link
 * OutboundStore#removeHandle(byte[])} / {@link OutboundStore#cleanupExpiredHandles(long)}.
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
  public void put(byte[] ohId, HandleRecord record) {
    String handleKey = Utils.bytesToHexString(ohId);
    owner.tx(
        () -> {
          handles.put(handleKey, record);
          owner.markDirty();
        });
  }

  public HandleRecord get(byte[] ohId) {
    String handleKey = Utils.bytesToHexString(ohId);
    return owner.read(() -> handles.get(handleKey));
  }

  /**
   * Returns the oh_ids of all non-expired handles (MS02b: used by the periodic DHT announce job).
   */
  public List<byte[]> listActiveOhIds(long now) {
    return owner.read(
        () -> {
          List<byte[]> result = new ArrayList<>();
          for (Map.Entry<String, HandleRecord> entry : handles.entrySet()) {
            HandleRecord record = entry.getValue();
            if (record != null && record.getExpiresAtMs() >= now) {
              result.add(Hex.decode(entry.getKey()));
            }
          }
          return result;
        });
  }

  /**
   * Hex keys of all handles that expired before {@code now} — snapshot, safe to remove while
   * iterating it.
   */
  List<String> hexKeysExpiredBefore(long now) {
    return owner.read(
        () -> {
          List<String> result = new ArrayList<>();
          for (Map.Entry<String, HandleRecord> entry : handles.entrySet()) {
            HandleRecord record = entry.getValue();
            if (record != null && record.getExpiresAtMs() < now) {
              result.add(entry.getKey());
            }
          }
          return result;
        });
  }

  /**
   * Removes the handle only. Package-private on purpose: the mailbox of that handle must be deleted
   * in the same transaction, which is what {@link OutboundStore#removeHandle(byte[])} does.
   */
  void removeByHexKey(String ohIdHex) {
    owner.tx(
        () -> {
          if (handles.remove(ohIdHex) != null) {
            owner.markDirty();
          }
        });
  }
}
