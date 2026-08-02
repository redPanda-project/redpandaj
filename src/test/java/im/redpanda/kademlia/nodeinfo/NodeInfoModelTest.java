package im.redpanda.kademlia.nodeinfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import im.redpanda.core.NodeId;
import org.junit.jupiter.api.Test;

class NodeInfoModelTest {

  @Test
  void exportImportTest() {
    NodeId nodeId = NodeId.importPublic(new NodeId().exportPublic());

    GMEntryPointModel gmEntryPointModel = new GMEntryPointModel(nodeId);

    NodeInfoModel nodeInfoModel = new NodeInfoModel();
    nodeInfoModel.addEntryPoint(gmEntryPointModel);
    String export = nodeInfoModel.export();
    NodeInfoModel imported = NodeInfoModel.importFromString(export);

    assertEquals(1, imported.getEntryPoints().size());
    assertNotEquals(0L, imported.getTimestamp());

    GMEntryPointModel firstEntry = imported.getEntryPoints().getFirst();
    assertEquals(nodeId, firstEntry.getNodeId());
  }
}
