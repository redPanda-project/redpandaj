package im.redpanda.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import im.redpanda.outbound.v1.MailItem;
import java.io.File;
import java.util.List;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OutboundMailboxStoreTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private OutboundMailboxStore store;
  private byte[] ohId;

  @Before
  public void setUp() {
    store = new OutboundMailboxStore(); // Uses in-memory
    ohId = Hex.decode("123456");
  }

  private static MailItem msg(String payload) {
    return MailItem.newBuilder().setPayload(ByteString.copyFromUtf8(payload)).build();
  }

  // --- AC: MailItem has monotone sequence_id per OH ---

  @Test
  public void addMessage_assignsMonotoneSequenceIds() {
    MailItem item1 = MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("msg1")).build();
    MailItem item2 = MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("msg2")).build();

    store.addMessage(ohId, item1);
    store.addMessage(ohId, item2);

    List<MailItem> messages = store.fetchMessages(ohId, 10, 0);
    assertThat(messages).hasSize(2);
    assertThat(messages.get(0).getSequenceId()).isEqualTo(1L);
    assertThat(messages.get(1).getSequenceId()).isEqualTo(2L);
    assertThat(messages.get(0).getPayload().toStringUtf8()).isEqualTo("msg1");
    assertThat(messages.get(1).getPayload().toStringUtf8()).isEqualTo("msg2");
  }

  @Test
  public void addMessage_sequenceIdsAreIndependentPerOh() {
    byte[] ohId2 = Hex.decode("abcdef");

    store.addMessage(ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("a")).build());
    store.addMessage(ohId2, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("b")).build());
    store.addMessage(ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("c")).build());

    List<MailItem> ohItems = store.fetchMessages(ohId, 10, 0);
    assertThat(ohItems).hasSize(2);
    assertThat(ohItems.get(0).getSequenceId()).isEqualTo(1L);
    assertThat(ohItems.get(1).getSequenceId()).isEqualTo(2L);

    List<MailItem> oh2Items = store.fetchMessages(ohId2, 10, 0);
    assertThat(oh2Items).hasSize(1);
    assertThat(oh2Items.get(0).getSequenceId()).isEqualTo(1L);
  }

  // --- AC: fetchMessages with afterSequence cursor ---

  @Test
  public void fetchMessages_withAfterSequenceCursor_returnsOnlyNewerItems() {
    for (int i = 0; i < 5; i++) {
      store.addMessage(
          ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("msg" + i)).build());
    }

    // Fetch first 2 (afterSequence=0 → seqId > 0)
    List<MailItem> batch1 = store.fetchMessages(ohId, 2, 0);
    assertThat(batch1).hasSize(2);
    assertThat(batch1.get(0).getPayload().toStringUtf8()).isEqualTo("msg0");
    assertThat(batch1.get(0).getSequenceId()).isEqualTo(1L);

    // next_cursor = highest seqId = 2; fetch after seqId 2
    List<MailItem> batch2 = store.fetchMessages(ohId, 2, 2);
    assertThat(batch2).hasSize(2);
    assertThat(batch2.get(0).getPayload().toStringUtf8()).isEqualTo("msg2");
    assertThat(batch2.get(0).getSequenceId()).isEqualTo(3L);

    // next_cursor = 4; fetch after seqId 4
    List<MailItem> batch3 = store.fetchMessages(ohId, 2, 4);
    assertThat(batch3).hasSize(1);
    assertThat(batch3.get(0).getPayload().toStringUtf8()).isEqualTo("msg4");
    assertThat(batch3.get(0).getSequenceId()).isEqualTo(5L);
  }

  @Test
  public void fetchMessages_emptyMailbox_returnsEmptyList() {
    assertThat(store.fetchMessages(ohId, 10, 0)).isEmpty();
  }

  @Test
  public void fetchMessages_afterSequenceExceedsMax_returnsEmpty() {
    store.addMessage(ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("x")).build());
    assertThat(store.fetchMessages(ohId, 10, 100)).isEmpty();
  }

  // --- AC: deleteUpTo removes items with sequence_id <= ackedSeqId ---

  @Test
  public void deleteUpTo_removesItemsUpToSequenceId() {
    for (int i = 1; i <= 5; i++) {
      store.addMessage(
          ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("msg" + i)).build());
    }

    store.deleteUpTo(ohId, 3);

    List<MailItem> remaining = store.fetchMessages(ohId, 10, 0);
    assertThat(remaining).hasSize(2);
    assertThat(remaining.get(0).getSequenceId()).isEqualTo(4L);
    assertThat(remaining.get(1).getSequenceId()).isEqualTo(5L);
  }

  @Test
  public void deleteUpTo_allItems_leavesEmptyMailbox() {
    store.addMessage(ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("a")).build());
    store.addMessage(ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("b")).build());

    store.deleteUpTo(ohId, 2);

    assertThat(store.fetchMessages(ohId, 10, 0)).isEmpty();
  }

  @Test
  public void deleteUpTo_noop_whenSequenceIdBeforeFirstItem() {
    store.addMessage(ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("a")).build());

    store.deleteUpTo(ohId, 0);

    assertThat(store.fetchMessages(ohId, 10, 0)).hasSize(1);
  }

  // --- AC: deleteAll removes all items for an OH ---

  @Test
  public void deleteAll_removesAllItemsForOh() {
    byte[] ohId2 = Hex.decode("abcdef");
    store.addMessage(ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("a")).build());
    store.addMessage(ohId2, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("b")).build());

    store.deleteAll(ohId);

    assertThat(store.fetchMessages(ohId, 10, 0)).isEmpty();
    assertThat(store.fetchMessages(ohId2, 10, 0)).hasSize(1);
  }

  // --- MS02b AC: full mailbox rejects new deposits (reject-new) and sets overflow flag ---

  @Test
  public void addMessage_whenMailboxFull_rejectsNewAndSetsOverflowFlag() {
    // Fill to capacity
    for (int i = 0; i < OutboundMailboxStore.MAX_ITEMS_PER_MAILBOX; i++) {
      OutboundMailboxStore.AddResult result =
          store.addMessage(
              ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("val" + i)).build());
      assertThat(result).isEqualTo(OutboundMailboxStore.AddResult.ADDED);
    }

    // No overflow yet
    assertThat(store.checkAndClearOverflow(ohId)).isFalse();

    // One more is rejected — nothing stored is displaced
    OutboundMailboxStore.AddResult rejected =
        store.addMessage(
            ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("overflow")).build());

    assertThat(rejected).isEqualTo(OutboundMailboxStore.AddResult.REJECTED_MAILBOX_FULL);
    assertThat(store.checkAndClearOverflow(ohId)).isTrue();
    // After clearing, flag is gone
    assertThat(store.checkAndClearOverflow(ohId)).isFalse();
  }

  @Test
  public void overflowFlag_clearedAfterCheck() {
    for (int i = 0; i <= OutboundMailboxStore.MAX_ITEMS_PER_MAILBOX; i++) {
      store.addMessage(
          ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("v" + i)).build());
    }

    boolean first = store.checkAndClearOverflow(ohId);
    boolean second = store.checkAndClearOverflow(ohId);
    assertThat(first).isTrue();
    assertThat(second).isFalse();
  }

  @Test
  public void testMailboxLimit_rejectNewKeepsOldest() {
    int limit = OutboundMailboxStore.MAX_ITEMS_PER_MAILBOX;
    for (int i = 0; i < limit + 5; i++) {
      store.addMessage(
          ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("val" + i)).build());
    }

    List<MailItem> messages = store.fetchMessages(ohId, 1000, 0);
    assertThat(messages).hasSize(limit);

    // Reject-new: the first stored items survive, the 5 excess deposits were rejected
    assertThat(messages.get(0).getPayload().toStringUtf8()).isEqualTo("val0");
    assertThat(messages.get(limit - 1).getPayload().toStringUtf8()).isEqualTo("val" + (limit - 1));
  }

  @Test
  public void addMessage_afterAckFreesSpace_acceptsAgain() {
    int limit = OutboundMailboxStore.MAX_ITEMS_PER_MAILBOX;
    for (int i = 0; i < limit; i++) {
      store.addMessage(
          ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("val" + i)).build());
    }
    assertThat(
            store.addMessage(
                ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("x")).build()))
        .isEqualTo(OutboundMailboxStore.AddResult.REJECTED_MAILBOX_FULL);

    // Acknowledge (delete) the first 10 items — deposits are accepted again
    store.deleteUpTo(ohId, 10);
    assertThat(
            store.addMessage(
                ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("y")).build()))
        .isEqualTo(OutboundMailboxStore.AddResult.ADDED);
  }

  // --- MS02b AC: per-item size limit ---

  @Test
  public void addMessage_itemAboveSizeLimit_isRejected() {
    byte[] tooLarge = new byte[OutboundMailboxStore.MAX_ITEM_BYTES + 1];

    OutboundMailboxStore.AddResult result =
        store.addMessage(
            ohId, MailItem.newBuilder().setPayload(ByteString.copyFrom(tooLarge)).build());

    assertThat(result).isEqualTo(OutboundMailboxStore.AddResult.REJECTED_ITEM_TOO_LARGE);
    assertThat(store.fetchMessages(ohId, 10, 0)).isEmpty();
  }

  @Test
  public void addMessage_itemJustBelowSizeLimit_isAccepted() {
    // Leave room for the message_id/seq/timestamp fields of the serialized MailItem
    byte[] payload = new byte[OutboundMailboxStore.MAX_ITEM_BYTES - 64];

    OutboundMailboxStore.AddResult result =
        store.addMessage(
            ohId, MailItem.newBuilder().setPayload(ByteString.copyFrom(payload)).build());

    assertThat(result).isEqualTo(OutboundMailboxStore.AddResult.ADDED);
  }

  // --- MS02b AC: per-mailbox byte quota independent of item count ---

  // --- T40 (a): persisted sequence counter survives a restart of a fully-acked mailbox ---

  @Test
  public void addMessage_afterRestartOfFullyAckedMailbox_continuesSequence() throws Exception {
    String path = new File(tempFolder.newFolder(), "outbound_mailbox.mapdb").getAbsolutePath();

    OutboundMailboxStore first = new OutboundMailboxStore(path);
    try {
      first.addMessage(ohId, msg("m1"));
      List<MailItem> stored = first.fetchMessages(ohId, 10, 0);
      assertThat(stored).hasSize(1);
      assertThat(stored.get(0).getSequenceId()).isEqualTo(1L);

      // Ack (delete) everything — the mailbox is now empty, no surviving item carries the sequence
      first.deleteUpTo(ohId, 1);
      assertThat(first.fetchMessages(ohId, 10, 0)).isEmpty();
    } finally {
      first.close();
    }

    // Reopen on the same path: the counter must resume at 2, not restart at 1
    OutboundMailboxStore reopened = new OutboundMailboxStore(path);
    try {
      reopened.addMessage(ohId, msg("m2"));
      List<MailItem> stored = reopened.fetchMessages(ohId, 10, 0);
      assertThat(stored).hasSize(1);
      assertThat(stored.get(0).getSequenceId()).isEqualTo(2L);
    } finally {
      reopened.close();
    }
  }

  // --- T40 cleanup: deleteAllByHexKey drops the persisted counter, next mailbox life restarts at 1

  @Test
  public void deleteAllByHexKey_resetsPersistedCounter() throws Exception {
    String path = new File(tempFolder.newFolder(), "outbound_mailbox.mapdb").getAbsolutePath();

    OutboundMailboxStore first = new OutboundMailboxStore(path);
    try {
      first.addMessage(ohId, msg("m1"));
      first.addMessage(ohId, msg("m2"));
      assertThat(first.lastAssignedSeq(ohId)).isEqualTo(2L);

      // Handle-expiry cleanup path: the whole mailbox and its counter are wiped
      first.deleteAllByHexKey(Hex.toHexString(ohId));
      assertThat(first.lastAssignedSeq(ohId)).isZero();
    } finally {
      first.close();
    }

    // A fresh store on the same path assigns seq 1 again for that OH
    OutboundMailboxStore reopened = new OutboundMailboxStore(path);
    try {
      reopened.addMessage(ohId, msg("m3"));
      List<MailItem> stored = reopened.fetchMessages(ohId, 10, 0);
      assertThat(stored).hasSize(1);
      assertThat(stored.get(0).getSequenceId()).isEqualTo(1L);
    } finally {
      reopened.close();
    }
  }

  // --- T40 (b) unit-level: lastAssignedSeq tracks the highest id ever assigned ---

  @Test
  public void lastAssignedSeq_reflectsAssignmentsAndIsZeroForUnknownOh() {
    assertThat(store.lastAssignedSeq(ohId)).isZero();
    store.addMessage(ohId, msg("a"));
    assertThat(store.lastAssignedSeq(ohId)).isEqualTo(1L);
    store.addMessage(ohId, msg("b"));
    assertThat(store.lastAssignedSeq(ohId)).isEqualTo(2L);
    // Deleting items does not lower the last-assigned watermark
    store.deleteUpTo(ohId, 2);
    assertThat(store.lastAssignedSeq(ohId)).isEqualTo(2L);
  }

  @Test
  public void addMessage_byteQuotaReached_rejectsBeforeItemCap() {
    // ~63 KiB per item: the 4 MiB quota is hit after ~66 items, far below the 500-item cap
    byte[] payload = new byte[63 * 1024];

    int accepted = 0;
    OutboundMailboxStore.AddResult last = OutboundMailboxStore.AddResult.ADDED;
    for (int i = 0; i < OutboundMailboxStore.MAX_ITEMS_PER_MAILBOX; i++) {
      last =
          store.addMessage(
              ohId, MailItem.newBuilder().setPayload(ByteString.copyFrom(payload)).build());
      if (last != OutboundMailboxStore.AddResult.ADDED) {
        break;
      }
      accepted++;
    }

    assertThat(last).isEqualTo(OutboundMailboxStore.AddResult.REJECTED_BYTE_QUOTA);
    // Quota must bite around MAX_MAILBOX_BYTES / itemSize, well below the 500-item cap
    long approxItems = OutboundMailboxStore.MAX_MAILBOX_BYTES / (63 * 1024);
    assertThat(accepted)
        .isLessThan(OutboundMailboxStore.MAX_ITEMS_PER_MAILBOX)
        .isBetween((int) approxItems - 2, (int) approxItems);
    assertThat(store.checkAndClearOverflow(ohId)).isTrue();

    // Quota is byte-based: freeing items via ack makes room again
    store.deleteUpTo(ohId, 2);
    assertThat(
            store.addMessage(
                ohId, MailItem.newBuilder().setPayload(ByteString.copyFrom(payload)).build()))
        .isEqualTo(OutboundMailboxStore.AddResult.ADDED);
  }
}
