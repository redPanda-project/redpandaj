package im.redpanda.core;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Covers the setKeys protocol documented in docs/protocol/01-nodeid-keypair.md. */
public class NodeIdSetKeysTest {

  @Test
  public void setKeys_acceptsMatchingKeysAndRejectsDuplicateSet() throws Exception {
    NodeId withKeys = NodeId.generateWithSimpleKey();
    NodeId idOnly = new NodeId(withKeys.getKademliaId());

    idOnly.setKeys(withKeys);

    assertTrue(idOnly.hasKey());
    assertTrue(idOnly.hasPrivate());
    assertNotNull(idOnly.sign("test".getBytes()));

    // duplicate set is rejected
    assertThrows(RuntimeException.class, () -> idOnly.setKeys(withKeys));
  }

  @Test
  public void setKeys_rejectsNull() {
    NodeId withKeys = NodeId.generateWithSimpleKey();
    NodeId idOnly = new NodeId(withKeys.getKademliaId());

    RuntimeException thrown = assertThrows(RuntimeException.class, () -> idOnly.setKeys(null));
    assertNotNull(thrown);
  }

  @Test
  public void setKeys_rejectsWithoutKnownKademliaId() {
    NodeId withKeys = NodeId.generateWithSimpleKey();
    NodeId noId = new NodeId((KademliaId) null);

    assertThrows(RuntimeException.class, () -> noId.setKeys(withKeys));
  }

  @Test
  public void setKeys_rejectsMismatchingKeys() {
    NodeId withKeys = NodeId.generateWithSimpleKey();
    NodeId otherKeys = NodeId.generateWithSimpleKey();
    NodeId idOnly = new NodeId(withKeys.getKademliaId());

    assertThrows(NodeId.KeypairDoesNotMatchException.class, () -> idOnly.setKeys(otherKeys));
  }
}
