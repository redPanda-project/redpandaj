package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Guards the on-disk compatibility of pre-v22-removal {@code localSettings*.dat} files (sdd02 phase
 * 2, T12).
 *
 * <p>The fixture was serialized with the LAST code state that still contained the full {@code
 * LegacyNodeId} implementation (2026-07-19, before the removal); its {@code legacyIdentity} field
 * holds a real serialized brainpool instance, exactly like every node's settings file written
 * before the removal. If deserializing it fails — e.g. because {@code LegacyNodeId} was deleted or
 * its pinned {@code serialVersionUID} drifted — {@link LocalSettings#load(int)} would silently fall
 * back to FRESH settings and every deployed node would lose its identity (Kademlia standing, update
 * timestamps, node graph) on the next restart. This test failing means: do not ship.
 */
class LocalSettingsLegacyFixtureTest {

  @Test
  void load_datWithLegacyIdentity_preservesNodeIdentity() throws Exception {
    final String expectedIdentity;
    try (InputStream in =
        getClass().getResourceAsStream("/fixtures/localSettings_v22era.identity")) {
      assertNotNull(in, "identity fixture missing");
      expectedIdentity = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
    }

    final InputStream raw = getClass().getResourceAsStream("/fixtures/localSettings_v22era.dat");
    assertNotNull(raw, "settings fixture missing");
    try (raw;
        ObjectInputStream in = new ObjectInputStream(raw)) {
      LocalSettings settings = (LocalSettings) in.readObject();
      assertEquals(
          expectedIdentity,
          settings.getMyIdentity().getKademliaId().toString(),
          "node identity must survive deserialization of a pre-removal settings file");
    }
  }
}
