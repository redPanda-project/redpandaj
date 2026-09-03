package im.redpanda.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import im.redpanda.outbound.OutboundHandleStore.HandleRecord;
import im.redpanda.outbound.v1.MailItem;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.bouncycastle.util.encoders.Hex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * T109: the handle registry and the mailboxes are one transactional store. These tests pin the two
 * properties that the pre-T109 two-database layout could not give: a half-executed operation leaves
 * no inconsistent state, and the in-memory counters are projections that are rebuilt correctly.
 */
class OutboundStoreTest {

  @TempDir public File tempFolder;

  private static final byte[] OH_A = Hex.decode("aaaa");
  private static final byte[] OH_B = Hex.decode("bbbb");
  private static final byte[] AUTH_KEY = new byte[65];

  private static MailItem msg(String payload) {
    return MailItem.newBuilder().setPayload(ByteString.copyFromUtf8(payload)).build();
  }

  private static HandleRecord handle(long now, long ttlMs) {
    return new HandleRecord(AUTH_KEY, now, now + ttlMs);
  }

  private String dbPath() throws IOException {
    File dir = new File(tempFolder, "store-" + System.nanoTime());
    if (!dir.mkdirs()) {
      throw new IOException("could not create " + dir);
    }
    return new File(dir, "outbound.mapdb").getAbsolutePath();
  }

  // --- expiry cleanup: handle and mailbox go together, no re-alignment helper involved ---

  @Test
  void cleanupExpiredHandles_removesExpiredHandleWithItsMailboxAndKeepsTheRest() {
    OutboundStore store = OutboundStore.inMemory();
    long now = System.currentTimeMillis();
    store.handles().put(OH_A, handle(now, 10_000));
    store.handles().put(OH_B, handle(now - 5_000, 4_000)); // expired 1 s ago
    store.mailbox().addMessage(OH_A, msg("keep"));
    store.mailbox().addMessage(OH_B, msg("drop"));

    assertThat(store.cleanupExpiredHandles(now)).isEqualTo(1);

    assertThat(store.handles().get(OH_B)).isNull();
    assertThat(store.mailbox().fetchMessages(OH_B, 10, 0)).isEmpty();
    assertThat(store.handles().get(OH_A)).isNotNull();
    assertThat(store.mailbox().fetchMessages(OH_A, 10, 0)).hasSize(1);
  }

  @Test
  void cleanupExpiredHandles_isANoopWithoutExpiredHandles() {
    OutboundStore store = OutboundStore.inMemory();
    long now = System.currentTimeMillis();
    store.handles().put(OH_A, handle(now, 10_000));

    assertThat(store.cleanupExpiredHandles(now)).isZero();
    assertThat(store.handles().get(OH_A)).isNotNull();
  }

  // --- atomicity: a revoke that fails half way leaves neither store changed ---

  @Test
  void failedRevoke_leavesHandleMailboxAndCountersUnchanged() throws Exception {
    String path = dbPath();
    OutboundStore store = OutboundStore.fileBacked(path);
    long usedBytes;
    try {
      store.handles().put(OH_A, handle(System.currentTimeMillis(), 60_000));
      store.mailbox().addMessage(OH_A, msg("m1"));
      store.mailbox().addMessage(OH_A, msg("m2"));
      usedBytes = store.mailbox().usedBytes(OH_A);
      assertThat(usedBytes).isPositive();

      // Same shape as OutboundService.handleRevoke, but the transaction fails after the handle and
      // the mailbox items have already been removed inside it.
      Runnable failingRevoke =
          () -> {
            store.removeHandle(OH_A);
            throw new IllegalStateException("boom");
          };
      assertThatThrownBy(() -> store.tx(failingRevoke))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("boom");

      // Nothing was applied: handle, items, byte counter and sequence watermark are all back.
      assertThat(store.handles().get(OH_A)).isNotNull();
      assertThat(store.mailbox().fetchMessages(OH_A, 10, 0)).hasSize(2);
      assertThat(store.mailbox().usedBytes(OH_A)).isEqualTo(usedBytes);
      assertThat(store.mailbox().lastAssignedSeq(OH_A)).isEqualTo(2L);
      // The rebuilt sequence projection keeps assigning after the watermark.
      store.mailbox().addMessage(OH_A, msg("m3"));
      assertThat(store.mailbox().fetchMessages(OH_A, 10, 2).get(0).getSequenceId()).isEqualTo(3L);
    } finally {
      store.close();
    }

    // And the same holds for the persisted state after a restart.
    OutboundStore reopened = OutboundStore.fileBacked(path);
    try {
      assertThat(reopened.handles().get(OH_A)).isNotNull();
      assertThat(reopened.mailbox().fetchMessages(OH_A, 10, 0)).hasSize(3);
      assertThat(reopened.mailbox().usedBytes(OH_A)).isGreaterThan(usedBytes);
    } finally {
      reopened.close();
    }
  }

  @Test
  void failedRegisterWithDeposit_persistsNeitherHandleNorItem() throws Exception {
    String path = dbPath();
    OutboundStore store = OutboundStore.fileBacked(path);
    try {
      // One transaction writing to both maps — the pre-T109 layout would have committed the handle
      // (first database) and lost the item (second database), or vice versa.
      Runnable failingRegister =
          () -> {
            store.handles().put(OH_A, handle(System.currentTimeMillis(), 60_000));
            store.mailbox().addMessage(OH_A, msg("m1"));
            throw new IllegalStateException("boom");
          };
      assertThatThrownBy(() -> store.tx(failingRegister)).isInstanceOf(IllegalStateException.class);

      assertThat(store.handles().get(OH_A)).isNull();
      assertThat(store.mailbox().fetchMessages(OH_A, 10, 0)).isEmpty();
      assertThat(store.mailbox().lastAssignedSeq(OH_A)).isZero();
      assertThat(store.mailbox().usedBytes(OH_A)).isZero();
    } finally {
      store.close();
    }

    OutboundStore reopened = OutboundStore.fileBacked(path);
    try {
      assertThat(reopened.handles().get(OH_A)).isNull();
      assertThat(reopened.mailbox().fetchMessages(OH_A, 10, 0)).isEmpty();
      assertThat(reopened.mailbox().lastAssignedSeq(OH_A)).isZero();
    } finally {
      reopened.close();
    }
  }

  @Test
  void successfulRevoke_removesHandleAndMailboxAcrossRestart() throws Exception {
    String path = dbPath();
    OutboundStore store = OutboundStore.fileBacked(path);
    try {
      store.handles().put(OH_A, handle(System.currentTimeMillis(), 60_000));
      store.mailbox().addMessage(OH_A, msg("m1"));
      store.removeHandle(OH_A);
      assertThat(store.handles().get(OH_A)).isNull();
      assertThat(store.mailbox().fetchMessages(OH_A, 10, 0)).isEmpty();
    } finally {
      store.close();
    }

    OutboundStore reopened = OutboundStore.fileBacked(path);
    try {
      assertThat(reopened.handles().get(OH_A)).isNull();
      assertThat(reopened.mailbox().fetchMessages(OH_A, 10, 0)).isEmpty();
      // T40: the mailbox was dropped with its handle, so a re-registered mailbox starts at 1 again
      assertThat(reopened.mailbox().lastAssignedSeq(OH_A)).isZero();
    } finally {
      reopened.close();
    }
  }

  // --- the deposit path must not race a revoke into an orphaned mailbox ---

  @Test
  void concurrentDepositAndRevoke_neverLeaveAnItemWithoutItsHandle() throws Exception {
    OutboundStore store = OutboundStore.inMemory();
    OutboundService service = new OutboundService(store);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 200; round++) {
        byte[] ohId = Hex.decode(String.format("%08x", round));
        store.handles().put(ohId, handle(System.currentTimeMillis(), 60_000));
        CyclicBarrier start = new CyclicBarrier(2);
        Future<?> deposit =
            pool.submit(
                () -> {
                  start.await();
                  service.depositMessage(
                      ohId, "payload".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                  return null;
                });
        Future<?> revoke =
            pool.submit(
                () -> {
                  start.await();
                  store.removeHandle(ohId);
                  return null;
                });
        deposit.get(10, TimeUnit.SECONDS);
        revoke.get(10, TimeUnit.SECONDS);

        // Whichever order the two transactions ran in, the handle is gone afterwards and its
        // mailbox must be gone with it: either the deposit committed first and the revoke removed
        // both, or the revoke committed first and the deposit found no handle (NOT_FOUND). A
        // deposit that checked the handle before the revoke and wrote the item after it would
        // leave an item nothing can ever reach or clean up.
        assertThat(store.handles().get(ohId)).isNull();
        assertThat(store.mailbox().fetchMessages(ohId, 10, 0))
            .as("orphaned mailbox items for round %d", round)
            .isEmpty();
      }
    } finally {
      pool.shutdownNow();
    }
  }

  // --- projections: sequence and byte counters are rebuilt from the persisted state on open ---

  @Test
  void restart_rebuildsSequenceAndByteCountersFromPersistedState() throws Exception {
    String path = dbPath();
    long usedBytes;
    OutboundStore store = OutboundStore.fileBacked(path);
    try {
      store.handles().put(OH_A, handle(System.currentTimeMillis(), 60_000));
      store.mailbox().addMessage(OH_A, msg("m1"));
      store.mailbox().addMessage(OH_A, msg("m2"));
      usedBytes = store.mailbox().usedBytes(OH_A);
    } finally {
      store.close();
    }

    OutboundStore reopened = OutboundStore.fileBacked(path);
    try {
      assertThat(reopened.mailbox().usedBytes(OH_A)).isEqualTo(usedBytes);
      assertThat(reopened.mailbox().lastAssignedSeq(OH_A)).isEqualTo(2L);
      reopened.mailbox().addMessage(OH_A, msg("m3"));
      assertThat(reopened.mailbox().fetchMessages(OH_A, 10, 2).get(0).getSequenceId())
          .isEqualTo(3L);
      assertThat(reopened.mailbox().usedBytes(OH_A)).isGreaterThan(usedBytes);

      // Acking frees the byte projection again
      reopened.mailbox().deleteUpTo(OH_A, 3);
      assertThat(reopened.mailbox().usedBytes(OH_A)).isZero();
      assertThat(reopened.mailbox().lastAssignedSeq(OH_A)).isEqualTo(3L);
    } finally {
      reopened.close();
    }
  }

  // --- T117 / TD111: the handle records are explicit JSON, not Serializer.JAVA ---

  /**
   * The lease fields have to survive a restart exactly: an auth key that comes back wrong rejects
   * every fetch of that mailbox, an expiry that comes back wrong silently changes the lease.
   */
  @Test
  void handleRecordFieldsSurviveARestart() throws Exception {
    String path = dbPath();
    byte[] authKey = new byte[65];
    for (int i = 0; i < authKey.length; i++) {
      authKey[i] = (byte) (i + 1);
    }

    OutboundStore store = OutboundStore.fileBacked(path);
    store.handles().put(OH_A, new HandleRecord(authKey, 1_700_000_000_000L, 1_700_000_060_000L));
    store.close();

    OutboundStore reopened = OutboundStore.fileBacked(path);
    try {
      HandleRecord loaded = reopened.handles().get(OH_A);
      assertThat(loaded).isNotNull();
      assertThat(loaded.getOhAuthPublicKey()).isEqualTo(authKey);
      assertThat(loaded.getCreatedAtMs()).isEqualTo(1_700_000_000_000L);
      assertThat(loaded.getExpiresAtMs()).isEqualTo(1_700_000_060_000L);
    } finally {
      reopened.close();
    }
  }

  /** No node state may be Java-serialized any more (DDD review §5, T117/TD111). */
  @Test
  void handleStorePinsNoClassName() throws Exception {
    String path = dbPath();
    OutboundStore store = OutboundStore.fileBacked(path);
    store.handles().put(OH_A, handle(System.currentTimeMillis(), 60_000));
    store.close();

    String raw =
        new String(
            java.nio.file.Files.readAllBytes(java.nio.file.Path.of(path)),
            java.nio.charset.StandardCharsets.ISO_8859_1);

    assertThat(raw)
        .as("the handle record must be stored as its own JSON")
        .contains("ohAuthPublicKey");
    assertThat(raw)
        .as("no fully qualified class name may be pinned in the store")
        .doesNotContain("im.redpanda.outbound.OutboundHandleStore");
  }
}
