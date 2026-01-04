package im.redpanda.core;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.security.KeyPair;
import java.security.Security;
import org.junit.Test;

public class NodeIdSetKeyPairNegativeCasesTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void setKeyPair_rejectsNull() {
    KeyPair kp = NodeId.generateECKeys();
    KademliaId kad = NodeId.fromPublicKey(kp.getPublic());
    NodeId nodeId = new NodeId(kad);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> nodeId.setKeyPair(null));
    assertTrue(exception.getMessage().contains("must not be null"));
  }

  @Test
  public void setKeyPair_rejectsMismatchingKeyPair() {
    KeyPair kp = NodeId.generateECKeys();
    KademliaId kad = NodeId.fromPublicKey(kp.getPublic());
    NodeId nodeId = new NodeId(kad);

    KeyPair other = NodeId.generateECKeys();

    assertThrows(NodeId.KeypairDoesNotMatchException.class, () -> nodeId.setKeyPair(other));
  }
}
