package im.redpanda.outbound;

import static org.junit.Assert.assertEquals;

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

  @Test
  public void testAddAndFetchMessages() {
    MailItem item1 = MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("msg1")).build();
    MailItem item2 = MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("msg2")).build();

    store.addMessage(ohId, item1);
    store.addMessage(ohId, item2);

    List<MailItem> messages = store.fetchMessages(ohId, 10, 0);
    assertEquals(2, messages.size());
    assertEquals("msg1", messages.get(0).getPayload().toStringUtf8());
    assertEquals("msg2", messages.get(1).getPayload().toStringUtf8());
  }

  @Test
  public void testFetchWithCursorAndLimit() {
    for (int i = 0; i < 5; i++) {
      MailItem item = MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("msg" + i)).build();
      store.addMessage(ohId, item);
    }

    // Fetch first 2
    List<MailItem> batch1 = store.fetchMessages(ohId, 2, 0);
    assertEquals(2, batch1.size());
    assertEquals("msg0", batch1.get(0).getPayload().toStringUtf8());

    // Fetch next 2 (cursor 2)
    List<MailItem> batch2 = store.fetchMessages(ohId, 2, 2);
    assertEquals(2, batch2.size());
    assertEquals("msg2", batch2.get(0).getPayload().toStringUtf8());

    // Fetch rest (cursor 4)
    List<MailItem> batch3 = store.fetchMessages(ohId, 2, 4);
    assertEquals(1, batch3.size());
    assertEquals("msg4", batch3.get(0).getPayload().toStringUtf8());
  }

  @Test
  public void testMailboxLimit() {
    // Current limit is 500. Let's add 505 items.
    int limit = 500;
    for (int i = 0; i < limit + 5; i++) {
      MailItem item = MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("val" + i)).build();
      store.addMessage(ohId, item);
    }

    List<MailItem> messages = store.fetchMessages(ohId, 1000, 0);
    assertEquals(limit, messages.size());

    // Since it drops oldest (FIFO), the first one should be "val5" (indices 0-4
    // dropped)
    assertEquals("val5", messages.get(0).getPayload().toStringUtf8());
    assertEquals("val" + (limit + 4), messages.get(limit - 1).getPayload().toStringUtf8());
  }
}
