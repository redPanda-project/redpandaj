package im.redpanda.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class UpdaterKeyTest {

  /**
   * The baked-in updater public key must be a valid 64-byte MS03 public NodeId export — a broken
   * constant would silently disable update verification (getPublicUpdaterKey() returns null).
   */
  @Test
  public void publicUpdaterKeyIsImportable() {
    NodeId key = Updater.getPublicUpdaterKey();
    assertNotNull(key);
    assertEquals(64, key.exportPublic().length);
  }
}
