package im.redpanda.core;

import org.junit.Test;

import java.security.Security;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NodeIdTest {

    static {

        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void exportWithPrivate() {

        NodeId nodeId = new NodeId();

        byte[] bytes = nodeId.exportWithPrivate();
        assertTrue(bytes.length == 252);

    }

    @Test
    public void importWithPrivate() {


        NodeId nodeId = new NodeId();

        byte[] bytes = nodeId.exportWithPrivate();


        NodeId nodeId1 = NodeId.importWithPrivate(bytes);

        assertNotNull(nodeId1);

        assertTrue(nodeId1.equals(nodeId));


    }
}