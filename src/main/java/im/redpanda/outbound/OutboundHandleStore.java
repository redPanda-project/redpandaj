package im.redpanda.outbound;

import im.redpanda.core.Log;
import im.redpanda.core.ServerContext;
import im.redpanda.crypt.Utils;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.bouncycastle.util.encoders.Hex;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundHandleStore {

  private static final Logger logger = LoggerFactory.getLogger(OutboundHandleStore.class);

  private DB db;
  private Map<String, HandleRecord> handles;
  private final String dbPath;

  public static class HandleRecord implements Serializable {
    private static final long serialVersionUID = 1L;
    private byte[] ohAuthPublicKey;
    private long createdAtMs;
    private long expiresAtMs;
    private long lastSeenMs;

    public HandleRecord(byte[] ohAuthPublicKey, long createdAtMs, long expiresAtMs) {
      this.ohAuthPublicKey = ohAuthPublicKey;
      this.createdAtMs = createdAtMs;
      this.expiresAtMs = expiresAtMs;
      this.lastSeenMs = System.currentTimeMillis();
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

    public long getLastSeenMs() {
      return lastSeenMs;
    }
  }

  // Constructor for testing or manual usage
  public OutboundHandleStore() {
    this.dbPath = null;
    this.handles = new ConcurrentHashMap<>();
  }

  public OutboundHandleStore(ServerContext context) {
    this.dbPath = "data/outbound_handles_" + context.getPort() + ".mapdb";
    init();
  }

  @SuppressWarnings("unchecked")
  private void init() {
    if (dbPath == null) return;
    try {
      Files.createDirectories(Path.of("data"));
      db = DBMaker.fileDB(dbPath).transactionEnable().make();
      handles =
          (Map<String, HandleRecord>)
              db.hashMap("handles", Serializer.STRING, Serializer.JAVA).createOrOpen();
    } catch (Exception e) {
      Log.sentry(e);
      logger.error("Failed to initialize OutboundHandleStore DB", e);
      handles = new ConcurrentHashMap<>();
    }
  }

  public void put(byte[] ohId, HandleRecord record) {
    String handleKey = Utils.bytesToHexString(ohId);
    handles.put(handleKey, record);
    if (db != null) db.commit();
  }

  public HandleRecord get(byte[] ohId) {
    String handleKey = Utils.bytesToHexString(ohId);
    return handles.get(handleKey);
  }

  public void remove(byte[] ohId) {
    String handleKey = Utils.bytesToHexString(ohId);
    handles.remove(handleKey);
    if (db != null) db.commit();
  }

  /**
   * Returns the oh_ids of all non-expired handles (MS02b: used by the periodic DHT announce job).
   */
  public List<byte[]> listActiveOhIds(long now) {
    List<byte[]> result = new ArrayList<>();
    for (Map.Entry<String, HandleRecord> entry : handles.entrySet()) {
      HandleRecord record = entry.getValue();
      if (record != null && record.getExpiresAtMs() >= now) {
        result.add(Hex.decode(entry.getKey()));
      }
    }
    return result;
  }

  public void close() {
    if (db != null && !db.isClosed()) {
      db.close();
    }
  }

  public void cleanupExpired(long now) {
    cleanupExpired(now, null);
  }

  /**
   * Removes all expired handles. If {@code mailboxStore} is non-null, also deletes the associated
   * mailbox items for each removed handle.
   */
  public void cleanupExpired(long now, OutboundMailboxStore mailboxStore) {
    boolean changed = false;
    Iterator<Map.Entry<String, HandleRecord>> it = handles.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, HandleRecord> entry = it.next();
      HandleRecord rec = entry.getValue();
      if (rec != null && rec.getExpiresAtMs() < now) {
        it.remove();
        changed = true;
        if (mailboxStore != null) {
          mailboxStore.deleteAllByHexKey(entry.getKey());
        }
      }
    }
    if (changed && db != null) db.commit();
  }
}
