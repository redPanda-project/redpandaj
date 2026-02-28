package im.redpanda.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import im.redpanda.outbound.v1.MailItem;
import java.util.List;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Before;
import org.junit.Test;

public class OutboundMailboxStoreTest {

  private OutboundMailboxStore store;
  private byte[] ohId;

  @Before
  public void setUp() {
    store = new OutboundMailboxStore(); // Uses in-memory
    ohId = Hex.decode("123456");
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

  // --- AC: Mailbox overflow (>500 items) sets overflow flag ---

  @Test
  public void addMessage_whenMailboxFull_evictsOldestAndSetsOverflowFlag() {
    // Fill to capacity
    for (int i = 0; i < OutboundMailboxStore.MAX_ITEMS_PER_MAILBOX; i++) {
      store.addMessage(
          ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("val" + i)).build());
    }

    // No overflow yet
    assertThat(store.checkAndClearOverflow(ohId)).isFalse();

    // Add one more to trigger eviction
    store.addMessage(
        ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("overflow")).build());

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
  public void testMailboxLimit_fifoEviction() {
    int limit = OutboundMailboxStore.MAX_ITEMS_PER_MAILBOX;
    for (int i = 0; i < limit + 5; i++) {
      store.addMessage(
          ohId, MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("val" + i)).build());
    }

    List<MailItem> messages = store.fetchMessages(ohId, 1000, 0);
    assertThat(messages).hasSize(limit);

    // FIFO: oldest 5 (val0–val4) are evicted; first surviving is val5
    assertThat(messages.get(0).getPayload().toStringUtf8()).isEqualTo("val5");
    assertThat(messages.get(limit - 1).getPayload().toStringUtf8()).isEqualTo("val" + (limit + 4));
  }
}
