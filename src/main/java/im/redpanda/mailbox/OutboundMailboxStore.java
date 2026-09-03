package im.redpanda.mailbox;

import im.redpanda.outbound.v1.MailItem;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mailbox facade of the mailbox context: the stored items of every locally hosted OH.
 *
 * <p>T109: this class no longer owns a database. It is one of the two facades over the single
 * {@link OutboundStore} database; every write runs inside the owner's transaction, so an item write
 * and the matching handle write of the same operation commit together (or not at all).
 */
public class OutboundMailboxStore {

  private static final Logger logger = LoggerFactory.getLogger(OutboundMailboxStore.class);

  private final OutboundStore owner;

  /**
   * Composite-key mailbox: key = {@link OhId#toHex()} + ":" + zero-padded-19-digit-seqId, value =
   * MailItem.toByteArray(). BTreeMap gives lexicographic sort enabling efficient prefix range
   * queries per OH.
   */
  private final NavigableMap<String, byte[]> mailboxItems;

  /**
   * Persisted last-assigned sequence id per mailbox: ohId_hex → last assigned sequence id (T40).
   * Written through on every assignment so the sequence keeps climbing across a node restart even
   * after all items of a mailbox have been acked and deleted. Without it the counter would restart
   * at 1 and any light client holding a higher persisted cursor would never see later deposits.
   */
  private final Map<String, Long> seqCountersPersisted;

  /**
   * In-memory sequence counters: ohId_hex → next sequence id (1-based). Projection of {@link
   * #seqCountersPersisted} and the surviving items, see {@link #rebuildProjections()}.
   */
  private final ConcurrentHashMap<String, AtomicLong> seqCounters = new ConcurrentHashMap<>();

  /**
   * In-memory byte usage per mailbox: ohId_hex → total stored bytes (serialized MailItem sizes).
   * Projection of the stored items, see {@link #rebuildProjections()}.
   */
  private final ConcurrentHashMap<String, AtomicLong> byteCounters = new ConcurrentHashMap<>();

  /**
   * Transient overflow flags: ohId_hex of OHs that had deposits rejected (mailbox full or byte
   * quota reached) since the last fetch. Cleared by checkAndClearOverflow().
   *
   * <p>MS02b note: before MS02b this flag meant "oldest items were evicted (FIFO)". With reject-new
   * eviction nothing stored is ever displaced; the flag now signals "deposits were rejected", so
   * the client still learns that messages may be missing.
   *
   * <p>Deliberately not a projection of the stored state: it describes what happened since the last
   * fetch, so it is neither persisted nor restored by {@link #rebuildProjections()} — including
   * after a rollback, where a flag cleared by the failed transaction stays cleared (the client
   * loses one "deposits were rejected" hint, nothing that is stored). For the same reason {@link
   * #checkAndClearOverflow(OhId)} does not take the store lock: the set is thread-safe on its own
   * and is not part of the transactional state.
   */
  private final Set<String> overflowFlags = ConcurrentHashMap.newKeySet();

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

  OutboundMailboxStore(
      OutboundStore owner,
      NavigableMap<String, byte[]> mailboxItems,
      Map<String, Long> seqCountersPersisted) {
    this.owner = owner;
    this.mailboxItems = mailboxItems;
    this.seqCountersPersisted = seqCountersPersisted;
    rebuildProjections();
  }

  /**
   * (Re-)derives the in-memory counters from the persisted maps. Called on open and after a
   * rollback — the two moments where the projections may not match the stored state.
   *
   * <p>The sequence counter has two inputs: {@code max(persistedLastAssigned + 1,
   * maxSurvivingItemSeq + 1)} (T40). A fully-acked mailbox has no surviving items, so only the
   * persisted value keeps the sequence from restarting at 1 after a restart.
   */
  void rebuildProjections() {
    seqCounters.clear();
    byteCounters.clear();
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
    for (Map.Entry<String, Long> entry : seqCountersPersisted.entrySet()) {
      String ohKey = entry.getKey();
      long lastAssigned = entry.getValue();
      seqCounters
          .computeIfAbsent(ohKey, k -> new AtomicLong(1L))
          .updateAndGet(current -> Math.max(current, lastAssigned + 1));
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
  public AddResult addMessage(OhId ohId, MailItem item) {
    String ohKey = ohId.toHex();
    return owner.tx(
        () -> {
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
          // T40: persist the just-assigned sequence id so the counter survives a restart even after
          // the item is later acked and deleted. Rides the same commit as the item write.
          seqCountersPersisted.put(ohKey, seqId);
          owner.markDirty();
          return AddResult.ADDED;
        });
  }

  private long nextSeqId(String ohKey) {
    return seqCounters.computeIfAbsent(ohKey, k -> new AtomicLong(1L)).getAndIncrement();
  }

  /**
   * Fetches up to {@code limit} items with {@code sequence_id > afterSequence}, ascending by
   * sequence_id.
   *
   * @param afterSequence 0 = from start; otherwise the last acknowledged sequence_id
   */
  public List<MailItem> fetchMessages(OhId ohId, int limit, long afterSequence) {
    String ohKey = ohId.toHex();
    return owner.read(
        () -> {
          String fromKey = itemKey(ohKey, afterSequence + 1);
          NavigableMap<String, byte[]> sub =
              mailboxItems.subMap(fromKey, true, ohCeiling(ohKey), false);

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
        });
  }

  /** Legacy overload — fetches from start (afterSequence = 0). */
  public List<MailItem> fetchMessages(OhId ohId, int limit) {
    return fetchMessages(ohId, limit, 0);
  }

  /**
   * Deletes all items with {@code sequence_id <= sequenceId} for the given OH.
   *
   * <p>Used by AckFetch to implement delete-after-acknowledge.
   */
  public void deleteUpTo(OhId ohId, long sequenceId) {
    String ohKey = ohId.toHex();
    owner.tx(
        () -> {
          String fromKey = ohPrefix(ohKey);
          String toKey = itemKey(ohKey, sequenceId);
          NavigableMap<String, byte[]> toDelete = mailboxItems.subMap(fromKey, true, toKey, true);
          Iterator<Map.Entry<String, byte[]>> it = toDelete.entrySet().iterator();
          long freedBytes = 0;
          boolean changed = false;
          while (it.hasNext()) {
            freedBytes += it.next().getValue().length;
            it.remove();
            changed = true;
          }
          subtractBytes(ohKey, freedBytes);
          if (changed) {
            // Explicitly "an item was removed", not "freedBytes > 0": the removal must be committed
            // even if the items happened to serialize to zero bytes.
            owner.markDirty();
          }
        });
  }

  /**
   * Deletes all items for the given OH. Package-private: dropping a whole mailbox is only correct
   * together with its handle, which is what {@link OutboundStore#removeHandle(OhId)} does in one
   * transaction.
   */
  void deleteAll(OhId ohId) {
    String ohIdHex = ohId.toHex();
    owner.tx(
        () -> {
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
          // T40: this is the handle-removal path — the whole mailbox is gone and the client is
          // forced through NOT_FOUND, which resets its cursor to 0 on re-register. Drop the
          // sequence counter so a re-registered mailbox starts fresh at 1. Only removed here.
          seqCounters.remove(ohIdHex);
          // Separate statement on purpose: inside the || the removal would be short-circuited away
          // whenever items were deleted, leaving the persisted watermark behind.
          boolean counterRemoved = seqCountersPersisted.remove(ohIdHex) != null;
          if (changed || counterRemoved) {
            owner.markDirty();
          }
        });
  }

  /**
   * T40: the last sequence id ever assigned for this OH (0 if none). Used by the fetch handler to
   * detect a stale client cursor that is higher than anything ever stored — a symptom of a
   * pre-persistence node restart — and heal it by resetting to 0.
   */
  public long lastAssignedSeq(OhId ohId) {
    String ohKey = ohId.toHex();
    return owner.read(
        () -> {
          AtomicLong counter = seqCounters.get(ohKey);
          // seqCounters holds the next (1-based) id to assign, so last assigned = next - 1.
          return counter == null ? 0L : counter.get() - 1;
        });
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

  /** Bytes currently accounted for this OH — the in-memory projection, for tests. */
  long usedBytes(OhId ohId) {
    String ohKey = ohId.toHex();
    return owner.read(
        () -> {
          AtomicLong counter = byteCounters.get(ohKey);
          return counter == null ? 0L : counter.get();
        });
  }

  /**
   * Returns {@code true} if deposits into this OH's mailbox were rejected (mailbox full or byte
   * quota reached) since the last call, and clears the overflow flag. This flag is transient — not
   * persisted across restarts.
   */
  public boolean checkAndClearOverflow(OhId ohId) {
    return overflowFlags.remove(ohId.toHex());
  }
}
