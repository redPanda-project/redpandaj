package im.redpanda.outbound;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
    OutboundHandleStore.HandleRecord record =
        new OutboundHandleStore.HandleRecord(authKey, created, expires);

    store.put(ohId, record);

    OutboundHandleStore.HandleRecord retrieved = store.get(ohId);
    assertNotNull(retrieved);
    assertEquals(created, retrieved.createdAtMs);
    assertEquals(expires, retrieved.expiresAtMs);
    // Array comparison needs more than equals for objects, but let's check length
    // or content if possible
    // or assume instance correctness. For byte arrays, standard equals checks
    // reference.
    // Let's check hex string of key if we want deep check, or just trust the object
    // ref if in-memory.
    // Wait, mapdb serializer would copy. In-memory concurrent map keeps ref.
    assertEquals(authKey, retrieved.ohAuthPublicKey);
  }

  @Test
  public void testRemove() {
    long created = System.currentTimeMillis();
    OutboundHandleStore.HandleRecord record =
        new OutboundHandleStore.HandleRecord(authKey, created, created + 10000);
    store.put(ohId, record);
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
}
