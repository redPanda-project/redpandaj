package im.redpanda.outbound;

import im.redpanda.core.Log;
import im.redpanda.core.ServerContext;
import im.redpanda.crypt.Utils;
import im.redpanda.outbound.v1.MailItem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboundMailboxStore {

  private static final Logger logger = LoggerFactory.getLogger(OutboundMailboxStore.class);

  private DB db;

  /**
   * Composite-key mailbox: key = hex(ohId) + ":" + zero-padded-19-digit-seqId, value =
   * MailItem.toByteArray(). BTreeMap gives lexicographic sort enabling efficient prefix range
   * queries per OH.
   */
  private NavigableMap<String, byte[]> mailboxItems;

  /** In-memory sequence counters: ohId_hex → next sequence id (1-based). */
  private final ConcurrentHashMap<String, AtomicLong> seqCounters = new ConcurrentHashMap<>();

  /**
   * Transient overflow flags: ohId_hex of OHs that had FIFO eviction since the last fetch. Cleared
   * by checkAndClearOverflow().
   */
  private final Set<String> overflowFlags = ConcurrentHashMap.newKeySet();

  private final String dbPath;

  static final int MAX_ITEMS_PER_MAILBOX = 500;

  private static final String SEQ_FMT = "%019d";

  /** Constructor for testing (in-memory only). */
  public OutboundMailboxStore() {
    this.dbPath = null;
    this.mailboxItems = new TreeMap<>();
  }

  public OutboundMailboxStore(ServerContext context) {
    this.dbPath = "data/outbound_mailbox_" + context.getPort() + ".mapdb";
    init();
  }

  @SuppressWarnings("unchecked")
  private void init() {
    if (dbPath == null) return;
    try {
      Files.createDirectories(Path.of("data"));
      db = DBMaker.fileDB(dbPath).transactionEnable().make();
      mailboxItems =
          db.treeMap("mailboxItemsV2", Serializer.STRING, Serializer.BYTE_ARRAY).createOrOpen();
      // Restore in-memory sequence counters from persisted keys
      for (String key : mailboxItems.keySet()) {
        int sep = key.lastIndexOf(':');
        if (sep > 0) {
          String ohKey = key.substring(0, sep);
          long seqId = Long.parseLong(key.substring(sep + 1));
          seqCounters
              .computeIfAbsent(ohKey, k -> new AtomicLong(1L))
              .updateAndGet(current -> Math.max(current, seqId + 1));
        }
      }
    } catch (Exception e) {
      Log.sentry(e);
      logger.error("Failed to initialize OutboundMailboxStore DB", e);
      mailboxItems = new TreeMap<>();
    }
  }

  private static String itemKey(String ohKey, long seqId) {
    return ohKey + ":" + String.format(SEQ_FMT, seqId);
  }

  private static String ohPrefix(String ohKey) {
    return ohKey + ":";
  }

  /**
   * Upper exclusive bound for all keys of ohKey. ";" (ASCII 59) > ":" (ASCII 58) and hex chars are
   * 0-9 and a-f, so this correctly bounds the range.
   */
  private static String ohCeiling(String ohKey) {
    return ohKey + ";";
  }

  private long nextSeqId(String ohKey) {
    return seqCounters.computeIfAbsent(ohKey, k -> new AtomicLong(1L)).getAndIncrement();
  }

  private long countItems(String ohKey) {
    return mailboxItems.subMap(ohPrefix(ohKey), ohCeiling(ohKey)).size();
  }

  /**
   * Adds a message to the mailbox for the given OH. Assigns a monotonically increasing sequence_id.
   * If the mailbox is full, the oldest item is evicted (FIFO) and the overflow flag is set.
   */
  public synchronized void addMessage(byte[] ohId, MailItem item) {
    String ohKey = Utils.bytesToHexString(ohId);

    // Enforce limit — evict oldest (FIFO)
    if (countItems(ohKey) >= MAX_ITEMS_PER_MAILBOX) {
      String firstKey = mailboxItems.ceilingKey(ohPrefix(ohKey));
      if (firstKey != null && firstKey.startsWith(ohPrefix(ohKey))) {
        mailboxItems.remove(firstKey);
        overflowFlags.add(ohKey);
      }
    }

    long seqId = nextSeqId(ohKey);
    MailItem itemWithSeq = item.toBuilder().setSequenceId(seqId).build();
    mailboxItems.put(itemKey(ohKey, seqId), itemWithSeq.toByteArray());
    if (db != null) db.commit();
  }

  /**
   * Fetches up to {@code limit} items with {@code sequence_id > afterSequence}, ascending by
   * sequence_id.
   *
   * @param afterSequence 0 = from start; otherwise the last acknowledged sequence_id
   */
  public synchronized List<MailItem> fetchMessages(byte[] ohId, int limit, long afterSequence) {
    String ohKey = Utils.bytesToHexString(ohId);
    String fromKey = itemKey(ohKey, afterSequence + 1);
    NavigableMap<String, byte[]> sub = mailboxItems.subMap(fromKey, true, ohCeiling(ohKey), false);

    List<MailItem> result = new ArrayList<>();
    for (byte[] bytes : sub.values()) {
      if (result.size() >= limit) break;
      try {
        result.add(MailItem.parseFrom(bytes));
      } catch (Exception e) {
        logger.error("Failed to parse MailItem", e);
      }
    }
    return result;
  }

  /** Legacy overload — fetches from start (afterSequence = 0). */
  public List<MailItem> fetchMessages(byte[] ohId, int limit) {
    return fetchMessages(ohId, limit, 0);
  }

  /**
   * Deletes all items with {@code sequence_id <= sequenceId} for the given OH and commits.
   *
   * <p>Used by AckFetch to implement delete-after-acknowledge.
   */
  public synchronized void deleteUpTo(byte[] ohId, long sequenceId) {
    String ohKey = Utils.bytesToHexString(ohId);
    String fromKey = ohPrefix(ohKey);
    String toKey = itemKey(ohKey, sequenceId);
    NavigableMap<String, byte[]> toDelete = mailboxItems.subMap(fromKey, true, toKey, true);
    Iterator<String> it = toDelete.keySet().iterator();
    boolean changed = false;
    while (it.hasNext()) {
      it.next();
      it.remove();
      changed = true;
    }
    if (db != null && changed) db.commit();
  }

  /**
   * Deletes all items for the given OH identified by its hex key. Used during expiry cleanup where
   * the hex key is already available, avoiding redundant re-encoding.
   */
  public synchronized void deleteAllByHexKey(String ohIdHex) {
    NavigableMap<String, byte[]> sub =
        mailboxItems.subMap(ohPrefix(ohIdHex), true, ohCeiling(ohIdHex), false);
    Iterator<String> it = sub.keySet().iterator();
    boolean changed = false;
    while (it.hasNext()) {
      it.next();
      it.remove();
      changed = true;
    }
    overflowFlags.remove(ohIdHex);
    if (db != null && changed) db.commit();
  }

  /** Deletes all items for the given OH. */
  public void deleteAll(byte[] ohId) {
    deleteAllByHexKey(Utils.bytesToHexString(ohId));
  }

  /**
   * Returns {@code true} if items were evicted from this OH's mailbox since the last call, and
   * clears the overflow flag. This flag is transient — not persisted across restarts.
   */
  public boolean checkAndClearOverflow(byte[] ohId) {
    return overflowFlags.remove(Utils.bytesToHexString(ohId));
  }

  public void close() {
    if (db != null && !db.isClosed()) {
      db.close();
    }
  }
}
