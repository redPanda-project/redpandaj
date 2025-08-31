package im.redpanda.core;

import org.junit.Test;

import java.security.KeyPair;
import java.security.Security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class NodeIdSetKeyPairTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void setKeyPair_acceptsMatchingKeyPair() throws Exception {
        // Generate a keypair and derive its KademliaId
        KeyPair kp = NodeId.generateECKeys();
        KademliaId kad = NodeId.fromPublicKey(kp.getPublic());

        // Create a NodeId with only the KademliaId set
        NodeId nodeId = new NodeId(kad);

        // Attempt to set the matching keypair should succeed (no exception)
        nodeId.setKeyPair(kp);

        assertNotNull(nodeId.getKeyPair());
        assertEquals(kad, nodeId.getKademliaId());
    }
}

