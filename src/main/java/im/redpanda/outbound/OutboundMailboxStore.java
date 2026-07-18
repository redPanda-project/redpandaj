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
import java.util.Map;
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
   * Persisted last-assigned sequence id per mailbox: ohId_hex → last assigned sequence id (T40).
   * Written through on every assignment so the sequence keeps climbing across a node restart even
   * after all items of a mailbox have been acked and deleted. Without it the counter would restart
   * at 1 and any light client holding a higher persisted cursor would never see later deposits.
   * {@code null} in the in-memory-only (test) mode where {@code db == null}.
   */
  private Map<String, Long> seqCountersPersisted;

  /**
   * In-memory byte usage per mailbox: ohId_hex → total stored bytes (serialized MailItem sizes).
   * Rebuilt from the persisted map on startup, updated on every add/delete.
   */
  private final ConcurrentHashMap<String, AtomicLong> byteCounters = new ConcurrentHashMap<>();

  /**
   * Transient overflow flags: ohId_hex of OHs that had deposits rejected (mailbox full or byte
   * quota reached) since the last fetch. Cleared by checkAndClearOverflow().
   *
   * <p>MS02b note: before MS02b this flag meant "oldest items were evicted (FIFO)". With reject-new
   * eviction nothing stored is ever displaced; the flag now signals "deposits were rejected", so
   * the client still learns that messages may be missing.
   */
  private final Set<String> overflowFlags = ConcurrentHashMap.newKeySet();

  private final String dbPath;

  static final int MAX_ITEMS_PER_MAILBOX = 500;

  /**
   * Per-item limit on the serialized {@link MailItem} size. Deposits above this are rejected
   * (BAD_REQUEST): the 500-item cap alone counts items, not bytes, so a single item could otherwise
   * be arbitrarily large.
   */
  public static final int MAX_ITEM_BYTES = 64 * 1024;

  /**
   * Byte quota per mailbox, independent of the item count. Deposits that would exceed it are
   * rejected (QUOTA_EXCEEDED).
   */
  static final long MAX_MAILBOX_BYTES = 4L * 1024 * 1024;

  /** Result of {@link #addMessage}: deposited, or rejected with the reason (MS02b hardening). */
  public enum AddResult {
    ADDED,
    REJECTED_ITEM_TOO_LARGE,
    REJECTED_MAILBOX_FULL,
    REJECTED_BYTE_QUOTA
  }

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

  /** Test constructor: file-backed store at an explicit path (restart/persistence tests). */
  OutboundMailboxStore(String dbPath) {
    this.dbPath = dbPath;
    init();
  }

  @SuppressWarnings("unchecked")
  private void init() {
    if (dbPath == null) return;
    try {
      Path parent = Path.of(dbPath).getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      db = DBMaker.fileDB(dbPath).transactionEnable().make();
      mailboxItems =
          db.treeMap("mailboxItemsV2", Serializer.STRING, Serializer.BYTE_ARRAY).createOrOpen();
      seqCountersPersisted =
          db.hashMap("seqCountersV1", Serializer.STRING, Serializer.LONG).createOrOpen();
      // Restore in-memory sequence and byte counters from persisted entries
      for (Map.Entry<String, byte[]> entry : mailboxItems.entrySet()) {
        String key = entry.getKey();
        int sep = key.lastIndexOf(':');
        if (sep > 0) {
          String ohKey = key.substring(0, sep);
          long seqId = Long.parseLong(key.substring(sep + 1));
          seqCounters
              .computeIfAbsent(ohKey, k -> new AtomicLong(1L))
              .updateAndGet(current -> Math.max(current, seqId + 1));
          byteCounters
              .computeIfAbsent(ohKey, k -> new AtomicLong(0L))
              .addAndGet(entry.getValue().length);
        }
      }
      // T40: restore the sequence counter from the persisted last-assigned id as well. This is the
      // second, authoritative input: max(persistedLastAssigned + 1, maxSurvivingItemSeq + 1). A
      // fully-acked mailbox has no surviving items, so only the persisted value keeps the sequence
      // from restarting at 1 after a restart.
      for (Map.Entry<String, Long> entry : seqCountersPersisted.entrySet()) {
        String ohKey = entry.getKey();
        long lastAssigned = entry.getValue();
        seqCounters
            .computeIfAbsent(ohKey, k -> new AtomicLong(1L))
            .updateAndGet(current -> Math.max(current, lastAssigned + 1));
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
   *
   * <p>MS02b deposit hardening — reject-new instead of drop-oldest: a deposit into a full mailbox
   * (item cap or byte quota) is rejected and the overflow flag is set, but already-stored items are
   * never displaced. Spam can block a full mailbox, but cannot silently flush real messages.
   *
   * @return {@link AddResult#ADDED} or the rejection reason
   */
  public synchronized AddResult addMessage(byte[] ohId, MailItem item) {
    String ohKey = Utils.bytesToHexString(ohId);

    long seqId = seqCounters.computeIfAbsent(ohKey, k -> new AtomicLong(1L)).get();
    byte[] serialized = item.toBuilder().setSequenceId(seqId).build().toByteArray();

    if (serialized.length > MAX_ITEM_BYTES) {
      return AddResult.REJECTED_ITEM_TOO_LARGE;
    }
    if (countItems(ohKey) >= MAX_ITEMS_PER_MAILBOX) {
      overflowFlags.add(ohKey);
      return AddResult.REJECTED_MAILBOX_FULL;
    }
    AtomicLong usedBytes = byteCounters.computeIfAbsent(ohKey, k -> new AtomicLong(0L));
    if (usedBytes.get() + serialized.length > MAX_MAILBOX_BYTES) {
      overflowFlags.add(ohKey);
      return AddResult.REJECTED_BYTE_QUOTA;
    }

    nextSeqId(ohKey);
    mailboxItems.put(itemKey(ohKey, seqId), serialized);
    usedBytes.addAndGet(serialized.length);
    if (db != null) {
      // T40: persist the just-assigned sequence id so the counter survives a restart even after
      // the item is later acked and deleted. Rides the same commit as the item write. The null
      // check covers a partially failed init() (db opened, map creation failed) — the store then
      // degrades to the pre-T40 in-memory counter behavior instead of throwing.
      if (seqCountersPersisted != null) {
        seqCountersPersisted.put(ohKey, seqId);
      }
      db.commit();
    }
    return AddResult.ADDED;
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
    Iterator<Map.Entry<String, byte[]>> it = toDelete.entrySet().iterator();
    boolean changed = false;
    long freedBytes = 0;
    while (it.hasNext()) {
      freedBytes += it.next().getValue().length;
      it.remove();
      changed = true;
    }
    subtractBytes(ohKey, freedBytes);
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
    byteCounters.remove(ohIdHex);
    // T40: this is the handle-expiry path — the whole mailbox is gone and the client is forced
    // through NOT_FOUND, which resets its cursor to 0 on re-register. Drop the sequence counter so
    // a re-registered mailbox starts fresh at 1. Only removed here, never anywhere else.
    seqCounters.remove(ohIdHex);
    boolean counterRemoved = false;
    if (seqCountersPersisted != null) {
      counterRemoved = seqCountersPersisted.remove(ohIdHex) != null;
    }
    if (db != null && (changed || counterRemoved)) db.commit();
  }

  /**
   * T40: the last sequence id ever assigned for this OH (0 if none). Used by the fetch handler to
   * detect a stale client cursor that is higher than anything ever stored — a symptom of a
   * pre-persistence node restart — and heal it by resetting to 0.
   */
  public synchronized long lastAssignedSeq(byte[] ohId) {
    String ohKey = Utils.bytesToHexString(ohId);
    AtomicLong counter = seqCounters.get(ohKey);
    // seqCounters holds the next (1-based) id to assign, so last assigned = next - 1.
    return counter == null ? 0L : counter.get() - 1;
  }

  /** Reduces the in-memory byte counter for an OH, never going below zero. */
  private void subtractBytes(String ohKey, long freedBytes) {
    if (freedBytes <= 0) {
      return;
    }
    AtomicLong counter = byteCounters.get(ohKey);
    if (counter != null) {
      counter.updateAndGet(current -> Math.max(0, current - freedBytes));
    }
  }

  /** Deletes all items for the given OH. */
  public void deleteAll(byte[] ohId) {
    deleteAllByHexKey(Utils.bytesToHexString(ohId));
  }

  /**
   * Returns {@code true} if deposits into this OH's mailbox were rejected (mailbox full or byte
   * quota reached) since the last call, and clears the overflow flag. This flag is transient — not
   * persisted across restarts.
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
