package im.redpanda.core;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.security.KeyPair;
import java.security.Security;

public class NodeIdSetKeyPairAlreadySetTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void setKeyPair_rejectsWhenAlreadyInitialized() throws Exception {
        // NodeId with existing keyPair
        NodeId withKey = NodeId.generateWithSimpleKey();
        KeyPair another = NodeId.generateECKeys();

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("already set");
        withKey.setKeyPair(another);
    }
}

