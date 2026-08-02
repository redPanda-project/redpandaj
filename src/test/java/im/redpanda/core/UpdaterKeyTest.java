package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class UpdaterKeyTest {

  /**
   * The baked-in updater public key must be a valid 64-byte MS03 public NodeId export — a broken
   * constant would silently disable update verification (getPublicUpdaterKey() returns null).
   */
  @Test
  void publicUpdaterKeyIsImportable() {
    NodeId key = Updater.getPublicUpdaterKey();
    assertNotNull(key);
    assertEquals(NodeId.PUBLIC_KEYLEN, key.exportPublic().length);
  }
}
