package im.redpanda.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import org.bouncycastle.util.encoders.Hex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboundHandleStoreTest {

  private OutboundStore outboundStore;
  private OutboundHandleStore store;
  private byte[] ohId;
  private byte[] authKey;

  @BeforeEach
  void setUp() {
    outboundStore = OutboundStore.inMemory();
    store = outboundStore.handles();
    ohId = Hex.decode("123456");
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
}
