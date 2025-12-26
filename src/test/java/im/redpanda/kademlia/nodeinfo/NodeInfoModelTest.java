package im.redpanda.kademlia.nodeinfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import im.redpanda.core.NodeId;
import org.junit.Test;

public class NodeInfoModelTest {

  @Test
  public void exportImportTest() {
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
