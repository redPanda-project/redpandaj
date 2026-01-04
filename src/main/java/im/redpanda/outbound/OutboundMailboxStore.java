package im.redpanda.outbound;

import im.redpanda.core.Log;
import im.redpanda.core.ServerContext;
import im.redpanda.crypt.Utils;
import im.redpanda.outbound.v1.MailItem;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundMailboxStore {

  private static final Logger logger = LoggerFactory.getLogger(OutboundMailboxStore.class);

  private DB db;
  // PoC: Map<ohID_Hex, ArrayList<MailItemBytes>>
  // In production: Use a proper queue structure or separate map per mailbox to
  // avoid huge lists
  private Map<String, ArrayList<byte[]>> mailboxes;
  private final String dbPath;

  // Limits
  private static final int MAX_ITEMS_PER_MAILBOX = 500;

  // Constructor for testing
  public OutboundMailboxStore() {
    this.dbPath = null;
    this.mailboxes = new ConcurrentHashMap<>();
  }

  public OutboundMailboxStore(ServerContext context) {
    this.dbPath = "data/outbound_mailbox_" + context.getPort() + ".mapdb";
    init();
  }

  @SuppressWarnings("unchecked")
  private void init() {
    if (dbPath == null) return;
    try {
      new File("data").mkdirs();
      db = DBMaker.fileDB(dbPath).transactionEnable().make();
      // Using JAVA serializer for the list of byte arrays is simple but not most
      // efficient
      mailboxes =
          (Map<String, ArrayList<byte[]>>)
              db.hashMap("mailboxes", Serializer.STRING, Serializer.JAVA).createOrOpen();
    } catch (Exception e) {
      Log.sentry(e);
      logger.error("Failed to initialize OutboundMailboxStore DB", e);
      mailboxes = new ConcurrentHashMap<>();
    }
  }

  public void addMessage(byte[] ohId, MailItem item) {
    String key = Utils.bytesToHexString(ohId);
    ArrayList<byte[]> list = mailboxes.getOrDefault(key, new ArrayList<>());

    // Enforce limit
    if (list.size() >= MAX_ITEMS_PER_MAILBOX) {
      // Drop oldest? or reject? Standard behavior: drop oldest or simple FIFO
      list.remove(0);
    }

    list.add(item.toByteArray());
    mailboxes.put(key, list);
    if (db != null) db.commit();
  }

  public List<MailItem> fetchMessages(byte[] ohId, int limit) {
    String key = Utils.bytesToHexString(ohId);
    ArrayList<byte[]> list = mailboxes.get(key);
    List<MailItem> result = new ArrayList<>();

    if (list == null) return result;

    for (byte[] bytes : list) {
      try {
        result.add(MailItem.parseFrom(bytes));
        if (result.size() >= limit) break;
      } catch (Exception e) {
        logger.error("Failed to parse MailItem", e);
      }
    }
    return result;
  }

  // For PoC fetching just returns all or first N, and DOES NOT remove them?
  // Wait, typically fetch removes or we have a cursor.
  // The plan detailed "cursor".
  // "Map oh_id:seq -> MailItemRecord" was suggested.
  // Converting to that model is better.
  // But given constraints and existing code, I'll stick to simple list for now
  // but implement "Fetch" as "Peek".
  // Wait, if I don't implement "delete after fetch" or "cursor", client gets same
  // messages.
  // The plan said: "Client can: FETCH (Mailbox abrufen)... optional REVOKE".
  // And "Map oh_id -> QueueIndex ... Map oh_id:seq -> MailItemRecord"

  // Let's improve the store structure slightly to support cursor.
  // But for this first iteration (MVP/PoC phase 1), I will just return the list.
  // Ideally, 'FETCH' should imply getting messages.
  // The User plan said: "FetchResponse ... next_cursor".

  // I will just use the list index as cursor for simplicity.

  public List<MailItem> fetchMessages(byte[] ohId, int limit, long cursor) {
    String key = Utils.bytesToHexString(ohId);
    ArrayList<byte[]> list = mailboxes.get(key);
    List<MailItem> result = new ArrayList<>();

    if (list == null) return result;

    // Treat cursor as index
    int start = (int) cursor;
    if (start >= list.size()) return result;

    for (int i = start; i < list.size() && result.size() < limit; i++) {
      try {
        result.add(MailItem.parseFrom(list.get(i)));
      } catch (Exception e) {
        logger.error("Failed to parse MailItem", e);
      }
    }
    return result;
  }

  public void close() {
    if (db != null && !db.isClosed()) {
      db.close();
    }
  }
}
