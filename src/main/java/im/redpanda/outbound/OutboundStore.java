package im.redpanda.outbound;

import im.redpanda.core.Log;
import im.redpanda.core.ServerContext;
import im.redpanda.outbound.OutboundHandleStore.HandleRecord;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * T109: the single transactional owner of the mailbox-hosting state of this node (DDD review §4
 * N-MAILBOX, §7 Top-3 #3).
 *
 * <p>Before T109 the handle registry and the mailbox items lived in two independent MapDB files
 * with independent {@code commit()} calls. A revoke was two commits, so a crash in between left a
 * mailbox without its handle — items nobody can fetch and nobody deletes, because every cleanup
 * path iterates the handles. {@code OutboundHandleStore.cleanupExpired(now, mailboxStore)} existed
 * only to re-align the two files after such a divergence.
 *
 * <p>Now there is <b>one</b> MapDB file holding all three maps, and every write goes through {@link
 * #tx} — one commit per logical operation, rollback on failure. {@link OutboundHandleStore} and
 * {@link OutboundMailboxStore} stay as the two read/write facades over that one database; they no
 * longer own a database, a file or a commit.
 *
 * <h2>Invariants</h2>
 *
 * <ol>
 *   <li>A mailbox item exists only while its handle exists. Handle removal (revoke or expiry) and
 *       mailbox deletion are one transaction — see {@link #removeHandle} and {@link
 *       #cleanupExpiredHandles}. There is no API to remove a handle alone.
 *   <li>The persisted sequence watermark ({@code seqCountersV1}) is removed together with the items
 *       of that mailbox (T40 semantics: a re-registered mailbox starts at 1 again).
 *   <li>Every counter that is <i>not</i> in the database — the per-mailbox next-sequence and used-
 *       bytes counters — is a projection of the persisted maps, rebuilt on open and after a
 *       rollback (see {@link OutboundMailboxStore#rebuildProjections()}). The overflow flags are
 *       deliberately transient (they mean "deposits were rejected since the last fetch").
 * </ol>
 *
 * <p><b>Storage format:</b> the database file is {@code data/outbound_v2_<port>.mapdb}; every value
 * in it is explicit bytes (T117: the handle records are JSON in the {@code handlesV2} map, the
 * mailbox items already were). Older files ({@code outbound_<port>.mapdb} of T109, and its two
 * pre-T109 ancestors) are not read and not migrated (user decision 2026-09-01: no users yet, so
 * persisted node state may be dropped). A node that finds them logs a hint and starts with an empty
 * registry; clients heal via NOT_FOUND → re-register. The file name is bumped rather than only the
 * map name, because keeping the mailbox items of a file whose handles are unreadable would break
 * invariant 1 below.
 *
 * <p><b>In-memory mode</b> ({@link #inMemory()}, tests only) uses plain Java maps. It has the same
 * locking and the same projection rebuild, but no rollback — there is no database to roll back.
 */
public final class OutboundStore {

  private static final Logger logger = LoggerFactory.getLogger(OutboundStore.class);

  /** {@code null} in in-memory mode. */
  private final DB db;

  /**
   * The three persisted maps of this store. Not {@code final} only because the constructor falls
   * back to in-memory maps when the database cannot be opened; they are assigned exactly once and
   * never replaced afterwards.
   */
  private Map<String, HandleRecord> handleMap;

  private NavigableMap<String, byte[]> itemMap;
  private Map<String, Long> seqWatermarkMap;

  private final OutboundHandleStore handleStore;
  private final OutboundMailboxStore mailboxStore;

  /**
   * Guards every read and write of the maps as well as the commit, so a commit can never publish
   * another thread's half-applied operation. Reentrant: {@link #removeHandle} and {@link
   * #cleanupExpiredHandles} call facade methods that open a nested {@link #tx}.
   */
  private final ReentrantLock lock = new ReentrantLock();

  /** Nesting depth of {@link #tx}: only the outermost transaction commits. */
  private int txDepth;

  /** Set by {@link #markDirty()}: {@code false} means the transaction body wrote nothing. */
  private boolean dirty;

  private OutboundStore(String dbPath) {
    DB opened = null;
    if (dbPath != null) {
      try {
        Path parent = Path.of(dbPath).getParent();
        if (parent != null) {
          Files.createDirectories(parent);
        }
        opened = DBMaker.fileDB(dbPath).transactionEnable().make();
        handleMap = openHandles(opened);
        itemMap =
            opened
                .treeMap("mailboxItemsV2", Serializer.STRING, Serializer.BYTE_ARRAY)
                .createOrOpen();
        seqWatermarkMap =
            opened.hashMap("seqCountersV1", Serializer.STRING, Serializer.LONG).createOrOpen();
        // Commit the (possibly just created) map structures immediately. createOrOpen writes the
        // map roots inside the open transaction; without this commit the first rollback would
        // discard them and every later access to the map would fail with DBException$GetVoid.
        opened.commit();
      } catch (Exception e) {
        Log.sentry(e);
        logger.error("Failed to open the outbound store at {}, falling back to memory", dbPath, e);
        closeQuietly(opened);
        opened = null;
      }
    }
    if (opened == null) {
      // All-or-nothing fallback: either all three maps are database-backed or none of them is. A
      // mixed state (open database, in-memory map) would silently drop writes of one map on
      // restart.
      handleMap = new ConcurrentHashMap<>();
      itemMap = new TreeMap<>();
      seqWatermarkMap = new ConcurrentHashMap<>();
    }
    this.db = opened;
    this.handleStore = new OutboundHandleStore(this, handleMap);
    this.mailboxStore = new OutboundMailboxStore(this, itemMap, seqWatermarkMap);
  }

  /**
   * Explicit MapDB serializer for the handle records (T117/TD111): the record is stored as its own
   * JSON bytes instead of a Java object stream, so no fully qualified class name reaches the disk.
   */
  private static final Serializer<HandleRecord> HANDLE_RECORD_SERIALIZER =
      new Serializer<>() {
        @Override
        public void serialize(DataOutput2 out, HandleRecord value) throws IOException {
          Serializer.BYTE_ARRAY.serialize(out, value.toJsonBytes());
        }

        @Override
        public HandleRecord deserialize(DataInput2 in, int available) throws IOException {
          return HandleRecord.fromJsonBytes(Serializer.BYTE_ARRAY.deserialize(in, available));
        }
      };

  private static Map<String, HandleRecord> openHandles(DB db) {
    return db.hashMap("handlesV2", Serializer.STRING, HANDLE_RECORD_SERIALIZER).createOrOpen();
  }

  /** Opens (or creates) the outbound store of this node under {@code data/}. */
  public static OutboundStore forContext(ServerContext context) {
    int port = context.getPort();
    logStaleLegacyStores(port);
    return new OutboundStore("data/outbound_v2_" + port + ".mapdb");
  }

  /** In-memory store without persistence — for tests. */
  public static OutboundStore inMemory() {
    return new OutboundStore(null);
  }

  /** File-backed store at an explicit path — for restart/atomicity tests. */
  static OutboundStore fileBacked(String dbPath) {
    return new OutboundStore(dbPath);
  }

  /** The handle registry facade (oh_id → {@link HandleRecord}). */
  public OutboundHandleStore handles() {
    return handleStore;
  }

  /** The mailbox facade (items and sequence watermarks per oh_id). */
  public OutboundMailboxStore mailbox() {
    return mailboxStore;
  }

  /**
   * Removes a handle together with its whole mailbox in one transaction (revoke, and the per-handle
   * step of the expiry cleanup). This is the only way to remove a handle: invariant 1 above cannot
   * be violated by a caller, and a crash mid-way leaves either both or neither.
   */
  public void removeHandle(OhId ohId) {
    tx(
        () -> {
          handleStore.remove(ohId);
          mailboxStore.deleteAll(ohId);
        });
  }

  /**
   * Removes every handle that expired before {@code now} together with its mailbox, in one
   * transaction.
   *
   * <p>This replaces the pre-T109 {@code cleanupExpired(now, mailboxStore)} re-alignment: it is no
   * longer a repair job that reconciles two files, just the periodic expiry of leases (invariant 1
   * holds at every commit, so there is nothing left to re-align).
   *
   * @return the number of handles removed
   */
  public int cleanupExpiredHandles(long now) {
    return tx(
        () -> {
          // Snapshot first: the removals below mutate the handle map.
          List<OhId> expired = handleStore.expiredBefore(now);
          for (OhId ohId : expired) {
            removeHandle(ohId);
          }
          return expired.size();
        });
  }

  /** Runs {@code body} under the store lock without committing (read-only access to the maps). */
  <T> T read(Supplier<T> body) {
    lock.lock();
    try {
      return body.get();
    } finally {
      lock.unlock();
    }
  }

  /**
   * @see #tx(Supplier)
   */
  void tx(Runnable body) {
    tx(
        () -> {
          body.run();
          return null;
        });
  }

  /**
   * Runs {@code body} as one transaction: nested calls join the enclosing transaction, the
   * outermost one commits once (only if something was written, see {@link #markDirty()}). On any
   * throwable the database is rolled back and the in-memory projections are rebuilt from the
   * rolled-back maps, so a half-executed operation leaves no inconsistent state behind.
   *
   * <p>In-memory mode cannot roll the maps back; the projections are still rebuilt so they keep
   * matching whatever the maps hold.
   */
  <T> T tx(Supplier<T> body) {
    lock.lock();
    try {
      if (txDepth > 0) {
        return body.get();
      }
      txDepth = 1;
      try {
        T result = body.get();
        if (dirty && db != null) {
          db.commit();
        }
        return result;
      } catch (Throwable t) {
        rollback();
        throw t;
      } finally {
        txDepth = 0;
        dirty = false;
      }
    } finally {
      lock.unlock();
    }
  }

  /** Marks the running transaction as having written something, so the outermost call commits. */
  void markDirty() {
    dirty = true;
  }

  private void rollback() {
    if (db != null && !db.isClosed()) {
      try {
        db.rollback();
      } catch (Throwable t) {
        Log.sentry(t);
        logger.error("Rollback of the outbound store failed", t);
      }
    }
    mailboxStore.rebuildProjections();
  }

  /** Closes the database. Waits for a running transaction so no commit is cut in half. */
  public void close() {
    lock.lock();
    try {
      closeQuietly(db);
    } finally {
      lock.unlock();
    }
  }

  private static void closeQuietly(DB db) {
    if (db != null && !db.isClosed()) {
      db.close();
    }
  }

  /**
   * Logs the pre-T109 store files if they are still on disk. They are neither read nor migrated
   * (see the class comment) and can be deleted; the node comes up with an empty handle registry.
   */
  private static void logStaleLegacyStores(int port) {
    for (String legacy :
        new String[] {
          "data/outbound_handles_" + port + ".mapdb",
          "data/outbound_mailbox_" + port + ".mapdb",
          "data/outbound_" + port + ".mapdb"
        }) {
      if (Files.exists(Path.of(legacy))) {
        logger.info(
            "ignoring stale outbound store {}: it is no longer read and can be deleted", legacy);
      }
    }
  }
}
