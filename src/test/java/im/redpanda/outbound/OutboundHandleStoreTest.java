package im.redpanda.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.protobuf.ByteString;
import im.redpanda.outbound.v1.MailItem;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Before;
import org.junit.Test;

public class OutboundHandleStoreTest {

  private OutboundHandleStore store;
  private byte[] ohId;
  private byte[] authKey;

  @Before
  public void setUp() {
    store = new OutboundHandleStore(); // Uses in-memory
    ohId = Hex.decode("123456");
    authKey = Hex.decode("ABCDEF");
  }

  @Test
  public void testPutAndGet() {
    long created = System.currentTimeMillis();
    long expires = created + 10000;
    OutboundHandleStore.HandleRecord handleRecord =
        new OutboundHandleStore.HandleRecord(authKey, created, expires);

    store.put(ohId, handleRecord);

    OutboundHandleStore.HandleRecord retrieved = store.get(ohId);
    assertNotNull(retrieved);
    assertEquals(created, retrieved.getCreatedAtMs());
    assertEquals(expires, retrieved.getExpiresAtMs());
    // Array comparison needs more than equals for objects, but let's check length
    // or content if possible
    // or assume instance correctness. For byte arrays, standard equals checks
    // reference.
    // Let's check hex string of key if we want deep check, or just trust the object
    // ref if in-memory.
    // Wait, mapdb serializer would copy. In-memory concurrent map keeps ref.
    assertEquals(authKey, retrieved.getOhAuthPublicKey());
  }

  @Test
  public void testRemove() {
    long created = System.currentTimeMillis();
    OutboundHandleStore.HandleRecord handleRecord =
        new OutboundHandleStore.HandleRecord(authKey, created, created + 10000);
    store.put(ohId, handleRecord);
    assertNotNull(store.get(ohId));

    store.remove(ohId);
    assertNull(store.get(ohId));
  }

  @Test
  public void testCleanupExpired() {
    long now = System.currentTimeMillis();

    // Valid handle
    store.put(Hex.decode("1111"), new OutboundHandleStore.HandleRecord(authKey, now, now + 10000));

    // Expired handle
    store.put(
        Hex.decode("2222"), new OutboundHandleStore.HandleRecord(authKey, now - 5000, now - 1000));

    // Cleanup with time 'now' which is > now-1000
    store.cleanupExpired(now);

    assertNotNull(store.get(Hex.decode("1111")));
    assertNull(store.get(Hex.decode("2222")));
  }

  // --- MS02 AC: Expired OHs also have their mailboxes deleted ---

  @Test
  public void cleanupExpired_withMailboxStore_alsoDeletesMailbox() {
    long now = System.currentTimeMillis();
    OutboundMailboxStore mailboxStore = new OutboundMailboxStore();

    byte[] expiredOhId = Hex.decode("2222");
    byte[] validOhId = Hex.decode("1111");

    // Register valid and expired handles
    store.put(validOhId, new OutboundHandleStore.HandleRecord(authKey, now, now + 10_000));
    store.put(expiredOhId, new OutboundHandleStore.HandleRecord(authKey, now - 5_000, now - 1_000));

    // Deposit messages into both mailboxes
    MailItem msg = MailItem.newBuilder().setPayload(ByteString.copyFromUtf8("hello")).build();
    mailboxStore.addMessage(validOhId, msg);
    mailboxStore.addMessage(expiredOhId, msg);

    // Cleanup
    store.cleanupExpired(now, mailboxStore);

    // Expired handle and its mailbox should be gone
    assertThat(store.get(expiredOhId)).isNull();
    assertThat(mailboxStore.fetchMessages(expiredOhId, 10, 0)).isEmpty();

    // Valid handle and its mailbox should remain
    assertThat(store.get(validOhId)).isNotNull();
    assertThat(mailboxStore.fetchMessages(validOhId, 10, 0)).hasSize(1);
  }
}
