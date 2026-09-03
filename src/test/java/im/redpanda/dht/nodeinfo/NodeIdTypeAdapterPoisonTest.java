package im.redpanda.dht.nodeinfo;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import im.redpanda.core.NodeId;
import im.redpanda.crypt.Base58;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * REDPANDAJ-2EH (second occurrence, TD032): a nodeinfo KadContent is self-signed, so its JSON is
 * fully attacker-controlled. A Base58-valid entry-point node id string can decode to 64 bytes that
 * BouncyCastle rejects as a non-canonical / small-order Ed25519 point with {@code
 * IllegalArgumentException: invalid public key} — the same rejection #299 fixed at the handshake.
 * Before the fix that exception escaped {@link NodeInfoModel#importFromString} out of Gson and
 * unwound the whole relay path; here we pin that a poisoned entry is dropped while its siblings
 * survive.
 */
class NodeIdTypeAdapterPoisonTest {

  /** 64 bytes of 0xFF: the first 32 (Ed25519 verify key) are a non-canonical, rejected encoding. */
  private static String poisonNodeIdString() {
    byte[] poison = new byte[NodeId.PUBLIC_KEYLEN];
    Arrays.fill(poison, (byte) 0xFF);
    return Base58.encode(poison);
  }

  private static String validNodeIdString() {
    return Base58.encode(new NodeId().exportPublic());
  }

  /**
   * A single JSON entry point whose node id decodes to a crypto-invalid key must not blow up the
   * whole parse — it is dropped (null node id) and the model is returned.
   */
  @Test
  void poisonedEntryIsDroppedNotThrown() {
    String json =
        "{\"entryPoints\":[{\"nodeId\":\""
            + poisonNodeIdString()
            + "\",\"ip\":\"1.2.3.4\",\"port\":1234}],\"services\":[]}";

    NodeInfoModel model = assertDoesNotThrow(() -> NodeInfoModel.importFromString(json));
    assertNotNull(model);
    assertEquals(1, model.getEntryPoints().size());
    assertNull(model.getEntryPoints().getFirst().getNodeId(), "poisoned node id must be dropped");
  }

  /**
   * The core acceptance test: one poisoned entry point must not drop its valid siblings. Before the
   * fix Gson aborted the entire {@code entryPoints} array on the first crypto-invalid key, losing
   * every valid sibling in the same record.
   */
  @Test
  void poisonedEntryDoesNotDropItsSiblings() {
    String validA = validNodeIdString();
    String validB = validNodeIdString();

    String json =
        "{\"entryPoints\":["
            + "{\"nodeId\":\""
            + validA
            + "\",\"ip\":\"1.1.1.1\",\"port\":1},"
            + "{\"nodeId\":\""
            + poisonNodeIdString()
            + "\",\"ip\":\"2.2.2.2\",\"port\":2},"
            + "{\"nodeId\":\""
            + validB
            + "\",\"ip\":\"3.3.3.3\",\"port\":3}"
            + "],\"services\":[]}";

    NodeInfoModel model = assertDoesNotThrow(() -> NodeInfoModel.importFromString(json));
    assertNotNull(model);

    List<GMEntryPointModel> entryPoints = model.getEntryPoints();
    assertEquals(3, entryPoints.size(), "all three array elements must survive");

    // Both valid siblings keep their node id; only the poisoned middle entry is nulled out.
    assertNotNull(entryPoints.get(0).getNodeId(), "first valid sibling must survive");
    assertNull(entryPoints.get(1).getNodeId(), "poisoned entry must be dropped");
    assertNotNull(entryPoints.get(2).getNodeId(), "second valid sibling must survive");

    assertEquals("1.1.1.1", entryPoints.get(0).getIp());
    assertEquals("3.3.3.3", entryPoints.get(2).getIp());
  }

  /**
   * Copilot review follow-up: a non-string {@code nodeId} token (JSON null, number, object) used to
   * throw {@code IllegalStateException} from {@code nextString()} before the catch, aborting the
   * whole nodeinfo parse. It must be consumed and dropped like an unparseable node id.
   */
  @Test
  void nonStringNodeIdTokenIsDroppedNotThrown() {
    String valid = validNodeIdString();
    String json =
        "{\"entryPoints\":["
            + "{\"nodeId\":null,\"ip\":\"1.1.1.1\",\"port\":1},"
            + "{\"nodeId\":123,\"ip\":\"2.2.2.2\",\"port\":2},"
            + "{\"nodeId\":{\"a\":1},\"ip\":\"3.3.3.3\",\"port\":3},"
            + ("{\"nodeId\":\"" + valid + "\",\"ip\":\"4.4.4.4\",\"port\":4}")
            + "],\"services\":[]}";

    NodeInfoModel model = assertDoesNotThrow(() -> NodeInfoModel.importFromString(json));
    assertNotNull(model);
    assertEquals(4, model.getEntryPoints().size());
    assertNull(model.getEntryPoints().get(0).getNodeId());
    assertNull(model.getEntryPoints().get(1).getNodeId());
    assertNull(model.getEntryPoints().get(2).getNodeId());
    assertNotNull(model.getEntryPoints().get(3).getNodeId(), "valid sibling must survive");
  }

  /**
   * Copilot review follow-up: the {@code entryPoints} array itself may contain null elements — they
   * must parse into null list entries without blowing up (the consumer skips them).
   */
  @Test
  void nullArrayElementIsKept() {
    String json = "{\"entryPoints\":[null],\"services\":[]}";

    NodeInfoModel model = assertDoesNotThrow(() -> NodeInfoModel.importFromString(json));
    assertNotNull(model);
    assertEquals(1, model.getEntryPoints().size());
    assertNull(model.getEntryPoints().getFirst());
  }

  /** A well-formed record must still round-trip unchanged (no regression). */
  @Test
  void validRecordStillParses() {
    NodeId nodeId = new NodeId();
    NodeInfoModel source = new NodeInfoModel();
    source.addEntryPoint(new GMEntryPointModel(NodeId.importPublic(nodeId.exportPublic())));

    NodeInfoModel imported = NodeInfoModel.importFromString(source.export());

    assertEquals(1, imported.getEntryPoints().size());
    assertNotNull(imported.getEntryPoints().getFirst().getNodeId());
  }
}
