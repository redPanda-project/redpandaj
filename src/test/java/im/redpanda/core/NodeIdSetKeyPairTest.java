package im.redpanda.core;

import org.junit.Test;

import java.security.KeyPair;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class NodeIdSetKeyPairTest {

    @Test
    public void setsMatchingKeyPairOnce() throws Exception {
        KeyPair keyPair = NodeId.generateECKeys();
        KademliaId kademliaId = NodeId.fromPublicKey(keyPair.getPublic());
        NodeId nodeId = new NodeId(kademliaId);

        nodeId.setKeyPair(keyPair);

        assertSame(keyPair, nodeId.getKeyPair());
        RuntimeException second = assertThrows(RuntimeException.class, () -> nodeId.setKeyPair(keyPair));
        assertThat(second.getMessage(), is("keypair is already set for this NodeId!"));
    }

    @Test
    public void rejectsNullKeyPair() {
        KeyPair keyPair = NodeId.generateECKeys();
        KademliaId kademliaId = NodeId.fromPublicKey(keyPair.getPublic());
        NodeId nodeId = new NodeId(kademliaId);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> nodeId.setKeyPair(null));
        assertThat(thrown.getMessage(), is("provided keypair must not be null when setting NodeId keypair!"));
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
