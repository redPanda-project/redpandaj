package im.redpanda.core;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.security.KeyPair;
import java.security.Security;

public class NodeIdSetKeyPairNegativeCasesTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void setKeyPair_rejectsNull() throws Exception {
        KeyPair kp = NodeId.generateECKeys();
        KademliaId kad = NodeId.fromPublicKey(kp.getPublic());
        NodeId nodeId = new NodeId(kad);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("must not be null");
        nodeId.setKeyPair(null);
    }

    @Test
    public void setKeyPair_rejectsMismatchingKeyPair() throws Exception {
        KeyPair kp = NodeId.generateECKeys();
        KademliaId kad = NodeId.fromPublicKey(kp.getPublic());
        NodeId nodeId = new NodeId(kad);

        KeyPair other = NodeId.generateECKeys();

        thrown.expect(NodeId.KeypairDoesNotMatchException.class);
        nodeId.setKeyPair(other);
    }
}
