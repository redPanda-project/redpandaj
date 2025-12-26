package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.security.KeyPair;
import org.junit.Test;

public class NodeIdSetKeyPairTest {

  @Test
  public void setsMatchingKeyPairOnce() throws Exception {
    KeyPair keyPair = NodeId.generateECKeys();
    KademliaId kademliaId = NodeId.fromPublicKey(keyPair.getPublic());
    NodeId nodeId = new NodeId(kademliaId);

    nodeId.setKeyPair(keyPair);

    assertSame(keyPair, nodeId.getKeyPair());
    RuntimeException second =
        assertThrows(RuntimeException.class, () -> nodeId.setKeyPair(keyPair));
    assertThat(second).hasMessage("keypair is already set for this NodeId!");
  }

  @Test
  public void rejectsNullKeyPair() {
    KeyPair keyPair = NodeId.generateECKeys();
    KademliaId kademliaId = NodeId.fromPublicKey(keyPair.getPublic());
    NodeId nodeId = new NodeId(kademliaId);

    RuntimeException thrown = assertThrows(RuntimeException.class, () -> nodeId.setKeyPair(null));
    assertThat(thrown).hasMessage("provided keypair must not be null when setting NodeId keypair!");
  }

  @Test
  public void rejectsMismatchingKeyPair() {
    KeyPair expectedPair = NodeId.generateECKeys();
    KademliaId kademliaId = NodeId.fromPublicKey(expectedPair.getPublic());
    NodeId nodeId = new NodeId(kademliaId);

    KeyPair wrongPair = NodeId.generateECKeys();

    assertThrows(NodeId.KeypairDoesNotMatchException.class, () -> nodeId.setKeyPair(wrongPair));
  }
}
