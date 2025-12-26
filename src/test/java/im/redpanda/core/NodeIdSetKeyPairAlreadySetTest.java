package im.redpanda.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.security.KeyPair;
import java.security.Security;
import org.junit.Test;

public class NodeIdSetKeyPairAlreadySetTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void setKeyPair_rejectsWhenAlreadyInitialized() {
    // NodeId with existing keyPair
    NodeId withKey = NodeId.generateWithSimpleKey();
    KeyPair another = NodeId.generateECKeys();

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> withKey.setKeyPair(another));
    assertEquals("keypair is already set for this NodeId!", thrown.getMessage());
  }
}
