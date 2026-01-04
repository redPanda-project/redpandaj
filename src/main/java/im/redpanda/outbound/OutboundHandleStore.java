package im.redpanda.outbound;

import im.redpanda.core.Log;
import im.redpanda.core.ServerContext;
import im.redpanda.crypt.Utils;
import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundHandleStore {

  private static final Logger logger = LoggerFactory.getLogger(OutboundHandleStore.class);

  private DB db;
  private Map<String, HandleRecord> handles; // Key: Base58 or Hex of oh_id
  private final String dbPath;

  public static class HandleRecord implements Serializable {
    private static final long serialVersionUID = 1L;
    public byte[] ohAuthPublicKey;
    public long createdAtMs;
    public long expiresAtMs;
    public long lastSeenMs;

    public HandleRecord(byte[] ohAuthPublicKey, long createdAtMs, long expiresAtMs) {
      this.ohAuthPublicKey = ohAuthPublicKey;
      this.createdAtMs = createdAtMs;
      this.expiresAtMs = expiresAtMs;
      this.lastSeenMs = System.currentTimeMillis();
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

  private void init() {
    if (dbPath == null) return;
    try {
      new File("data").mkdirs();
      db = DBMaker.fileDB(dbPath).transactionEnable().make();
      handles = db.hashMap("handles", Serializer.STRING, Serializer.JAVA).createOrOpen();
    } catch (Exception e) {
      Log.sentry(e);
      logger.error("Failed to initialize OutboundHandleStore DB", e);
      // Fallback to memory if file fails? Or throw?
      // For now, let's just log and maybe fallback to memory map to keep running
      handles = new ConcurrentHashMap<>();
    }
  }

  public void put(byte[] ohId, HandleRecord record) {
    String key = Utils.bytesToHexString(ohId);
    handles.put(key, record);
    if (db != null) db.commit();
  }

  public HandleRecord get(byte[] ohId) {
    String key = Utils.bytesToHexString(ohId);
    return handles.get(key);
  }

  public void remove(byte[] ohId) {
    String key = Utils.bytesToHexString(ohId);
    handles.remove(key);
    if (db != null) db.commit();
  }

  public void close() {
    if (db != null && !db.isClosed()) {
      db.close();
    }
  }

  public void cleanupExpired(long now) {
    boolean changed = false;
    for (String key : handles.keySet()) {
      HandleRecord rec = handles.get(key);
      if (rec != null && rec.expiresAtMs < now) {
        handles.remove(key);
        changed = true;
      }
    }
    if (changed && db != null) db.commit();
  }
}
