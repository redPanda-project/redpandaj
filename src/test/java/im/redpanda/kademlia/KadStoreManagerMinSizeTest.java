package im.redpanda.kademlia;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.KademliaId;
import im.redpanda.core.ServerContext;
import java.lang.reflect.Field;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for L6 (bug hunt 2026-07-26): {@code MIN_SIZE} was declared as {@code 1024 * 1024
 * * 10 * 0}, so the periodic entry-eviction sweep was gated on {@code size > 0} and ran on every
 * put (throttled to ~10 s) instead of only once the store exceeds 10 MB.
 */
public class KadStoreManagerMinSizeTest {

  private static final long ONE_DAY_MS = 1000L * 60L * 60L * 24L;

  private ServerContext serverContext;
  private KadStoreManager kadStoreManager;

  @Before
  public void setUp() throws Exception {
    serverContext = ServerContext.buildDefaultServerContext();
    kadStoreManager = new KadStoreManager(serverContext);
    resetStore();
  }

  @After
  public void tearDown() throws Exception {
    resetStore();
  }

  /** Below the threshold the sweep must not run, so even a long-expired entry survives a put. */
  @Test
  public void put_doesNotSweepWhileStoreIsBelowMinSize() throws Exception {
    KadContent ancient = storeDirectly(100L * ONE_DAY_MS);

    assertThat(kadStoreManager.put(freshContent())).isTrue();

    assertThat(kadStoreManager.get(ancient.getId())).isNotNull();
  }

  /** Above the threshold the sweep runs and evicts entries past their keepTime. */
  @Test
  public void put_sweepsOnceStoreExceedsMinSize() throws Exception {
    KadContent ancient = storeDirectly(100L * ONE_DAY_MS);
    setStaticField("size", 11 * 1024 * 1024);

    assertThat(kadStoreManager.put(freshContent())).isTrue();

    assertThat(kadStoreManager.get(ancient.getId())).isNull();
  }

  private KadContent freshContent() {
    return new KadContent(System.currentTimeMillis(), randomKey(), new byte[16]);
  }

  /**
   * Puts an entry straight into the backing map: {@code put()} itself rejects anything older than
   * MAX_KEEP_TIME, and this entry has to be older than any possible keepTime.
   */
  @SuppressWarnings("unchecked")
  private KadContent storeDirectly(long ageMillis) throws Exception {
    KadContent content =
        new KadContent(System.currentTimeMillis() - ageMillis, randomKey(), new byte[16]);

    Field entriesField = KadStoreManager.class.getDeclaredField("entries");
    entriesField.setAccessible(true);
    ((Map<KademliaId, KadContent>) entriesField.get(null)).put(content.getId(), content);
    setStaticField("size", content.getContent().length);

    return content;
  }

  private static byte[] randomKey() {
    return new im.redpanda.core.NodeId().exportPublic();
  }

  @SuppressWarnings("unchecked")
  private static void resetStore() throws Exception {
    Field entriesField = KadStoreManager.class.getDeclaredField("entries");
    entriesField.setAccessible(true);
    ((Map<KademliaId, KadContent>) entriesField.get(null)).clear();

    setStaticField("size", 0);
    setStaticField("lastCleanup", 0L);
  }

  private static void setStaticField(String name, Object value) throws Exception {
    Field field = KadStoreManager.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(null, value);
  }
}
