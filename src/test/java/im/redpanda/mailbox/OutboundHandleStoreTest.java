package im.redpanda.mailbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.bouncycastle.util.encoders.Hex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboundHandleStoreTest {

  private OutboundStore outboundStore;
  private OutboundHandleStore store;
  private OhId ohId;
  private byte[] authKey;

  @BeforeEach
  void setUp() {
    outboundStore = OutboundStore.inMemory();
    store = outboundStore.handles();
    ohId = OhId.fromHex("12".repeat(OhId.GARLIC_BYTES));
    authKey = Hex.decode("ABCDEF");
  }

  @Test
  void putAndGet() {
    long created = System.currentTimeMillis();
    long expires = created + 10000;
    OutboundHandleStore.HandleRecord handleRecord =
        new OutboundHandleStore.HandleRecord(authKey, created, expires);

    store.put(ohId, handleRecord);

    OutboundHandleStore.HandleRecord retrieved = store.get(ohId);
    assertThat(retrieved).isNotNull();
    assertThat(retrieved.getCreatedAtMs()).isEqualTo(created);
    assertThat(retrieved.getExpiresAtMs()).isEqualTo(expires);
    assertThat(retrieved.getOhAuthPublicKey()).isEqualTo(authKey);
  }

  @Test
  void remove() {
    long created = System.currentTimeMillis();
    OutboundHandleStore.HandleRecord handleRecord =
        new OutboundHandleStore.HandleRecord(authKey, created, created + 10000);
    store.put(ohId, handleRecord);
    assertThat(store.get(ohId)).isNotNull();

    // T109: a handle is only ever removed together with its mailbox
    outboundStore.removeHandle(ohId);
    assertThat(store.get(ohId)).isNull();
  }

  @Test
  void listingsSkipPersistedKeysThatAreNoLongerValidOhIds() {
    // T129: a pre-T129 handle may have been registered with a 16..64-byte oh_id. Such a persisted
    // key must not abort the listings that drive the announce job and the expiry sweep.
    long now = System.currentTimeMillis();
    Map<String, OutboundHandleStore.HandleRecord> handles = new HashMap<>();
    String legacyKey = "34".repeat(32);
    handles.put(legacyKey, new OutboundHandleStore.HandleRecord(authKey, now, now + 10000));
    handles.put(ohId.toHex(), new OutboundHandleStore.HandleRecord(authKey, now, now + 10000));
    OutboundHandleStore legacyStore = new OutboundHandleStore(outboundStore, handles);

    assertThat(legacyStore.listActiveOhIds(now)).containsExactly(ohId);
    assertThat(legacyStore.expiredBefore(now + 20000)).containsExactly(ohId);
  }
}
