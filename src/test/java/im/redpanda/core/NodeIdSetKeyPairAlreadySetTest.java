package im.redpanda.core;

import org.junit.Test;

import java.security.KeyPair;
import java.security.Security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class NodeIdSetKeyPairAlreadySetTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void setKeyPair_rejectsWhenAlreadyInitialized() throws Exception {
        // NodeId with existing keyPair
        NodeId withKey = NodeId.generateWithSimpleKey();
        KeyPair another = NodeId.generateECKeys();

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> withKey.setKeyPair(another));
        assertEquals("keypair is already set for this NodeId!", thrown.getMessage());
    }
}
