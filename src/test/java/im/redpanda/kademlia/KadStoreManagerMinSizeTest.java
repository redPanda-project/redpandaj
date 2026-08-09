package im.redpanda.kademlia;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.KademliaId;
import im.redpanda.core.ServerContext;
import java.lang.reflect.Field;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for the {@code KadStoreManager} retention rules (L6 / bug hunt 2026-07-26 and
 * the T67 follow-up):
 *
 * <ul>
 *   <li>expiry at {@code MAX_KEEP_TIME} (14 days) is enforced regardless of store size — by the
 *       periodic sweep in {@code put()} and lazily in {@code get()} — so a node never serves a
 *       record that {@code put()} would reject as too old,
 *   <li>the shorter, distance-based {@code keepTime} only applies above the 10 MB {@code MIN_SIZE}
 *       gate (space pressure); below it, dropping the gate would collapse retention to the
 *       61-minute floor for typical entries (XOR distance ~160) and wipe small stores.
 * </ul>
 */
class KadStoreManagerMinSizeTest {

  private static final long ONE_DAY_MS = 1000L * 60L * 60L * 24L;

  private ServerContext serverContext;
  private KadStoreManager kadStoreManager;

  @BeforeEach
  void setUp() throws Exception {
    serverContext = ServerContext.buildDefaultServerContext();
    kadStoreManager = new KadStoreManager(serverContext);
    resetStore();
  }

  @AfterEach
  void tearDown() throws Exception {
    resetStore();
  }

  /**
   * Even below MIN_SIZE the sweep must drop entries older than MAX_KEEP_TIME: the map itself no
   * longer contains the entry after a put (not just filtered by get()).
   */
  @Test
  void put_evictsEntriesOlderThanMaxKeepTimeEvenBelowMinSize() throws Exception {
    KadContent ancient = storeDirectly(100L * ONE_DAY_MS, null);

    assertThat(kadStoreManager.put(freshContent())).isTrue();

    assertThat(rawEntries().containsKey(ancient.getId())).isFalse();
  }

  /**
   * Below MIN_SIZE the distance-based shortening must NOT apply: a 2-day-old entry at the maximum
   * XOR distance (keepTime would be the 61-minute floor under space pressure) survives the sweep.
   */
  @Test
  void put_doesNotApplyDistanceBasedKeepTimeBelowMinSize() throws Exception {
    KadContent recent = storeDirectly(2L * ONE_DAY_MS, idAtMaxDistanceFromUs());

    assertThat(kadStoreManager.put(freshContent())).isTrue();

    assertThat(kadStoreManager.get(recent.getId())).isNotNull();
  }

  /** Above MIN_SIZE the same 2-day-old, maximum-distance entry is evicted (keepTime 61 min). */
  @Test
  void put_appliesDistanceBasedKeepTimeAboveMinSize() throws Exception {
    KadContent recent = storeDirectly(2L * ONE_DAY_MS, idAtMaxDistanceFromUs());
    setStaticField("size", 11 * 1024 * 1024);

    assertThat(kadStoreManager.put(freshContent())).isTrue();

    assertThat(kadStoreManager.get(recent.getId())).isNull();
  }

  /**
   * The sweep only runs from put(): a node receiving no puts must still never serve an entry older
   * than MAX_KEEP_TIME — get() expires it lazily and removes it from the store.
   */
  @Test
  void get_expiresEntriesOlderThanMaxKeepTime() throws Exception {
    KadContent ancient = storeDirectly(100L * ONE_DAY_MS, null);

    assertThat(kadStoreManager.get(ancient.getId())).isNull();
    assertThat(rawEntries().containsKey(ancient.getId())).isFalse();
  }

  private KadContent freshContent() {
    return new KadContent(System.currentTimeMillis(), randomKey(), new byte[16]);
  }

  /**
   * An id whose most significant bit differs from our node id: XOR distance 160, the worst case for
   * the distance-based keepTime (clamped to the 61-minute floor).
   */
  private KademliaId idAtMaxDistanceFromUs() {
    byte[] bytes = serverContext.getNonce().getBytes().clone();
    bytes[0] ^= (byte) 0x80;
    return new KademliaId(bytes);
  }

  /**
   * Puts an entry straight into the backing map: {@code put()} itself rejects anything older than
   * MAX_KEEP_TIME. An optional forced id allows a deterministic XOR distance to our node id.
   */
  private KadContent storeDirectly(long ageMillis, KademliaId forcedId) throws Exception {
    KadContent content =
        new KadContent(System.currentTimeMillis() - ageMillis, randomKey(), new byte[16]);
    if (forcedId != null) {
      content.setId(forcedId);
    }

    rawEntries().put(content.getId(), content);
    setStaticField("size", content.getContent().length);

    return content;
  }

  private static byte[] randomKey() {
    return new im.redpanda.core.NodeId().exportPublic();
  }

  @SuppressWarnings("unchecked")
  private static Map<KademliaId, KadContent> rawEntries() throws Exception {
    Field entriesField = KadStoreManager.class.getDeclaredField("entries");
    entriesField.setAccessible(true);
    return (Map<KademliaId, KadContent>) entriesField.get(null);
  }

  private static void resetStore() throws Exception {
    rawEntries().clear();

    setStaticField("size", 0);
    setStaticField("lastCleanup", 0L);
  }

  private static void setStaticField(String name, Object value) throws Exception {
    Field field = KadStoreManager.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(null, value);
  }
}
