package im.redpanda.core;

import im.redpanda.crypt.Utils;
import org.junit.Test;

import java.security.Security;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NodeIdTest {

//    static {
//        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
//    }

    @Test
    public void exportWithPrivate() {

        NodeId nodeId = new NodeId();

        byte[] bytes = nodeId.exportWithPrivate();
        assertTrue(bytes.length == NodeId.PRIVATE_KEYLEN);

    }

    @Test
    public void importWithPrivate() {


        NodeId nodeId = new NodeId();

        byte[] bytes = nodeId.exportWithPrivate();


        NodeId nodeId1 = NodeId.importWithPrivate(bytes);

        assertNotNull(nodeId1);

        assertTrue(nodeId1.equals(nodeId));


    }

    @Test
    public void exportPublic() {


        NodeId nodeId = new NodeId();

        byte[] bytes = nodeId.exportPublic();

        assertTrue(bytes.length == NodeId.PUBLIC_KEYLEN);


    }

    @Test
    public void importPublic() {

        NodeId nodeId = new NodeId();

        byte[] bytes = nodeId.exportPublic();

        NodeId nodeId1 = NodeId.importPublic(bytes);

        assertTrue(nodeId1.equals(nodeId));

    }

    @Test
    public void testSignatures() {
        for (int i = 0; i < 1; i++) {

            byte[] bytes = "Test Message".getBytes();

            NodeId nodeId = new NodeId();
            byte[] signature = nodeId.sign(bytes);

            System.out.println("messagebytes: " + Utils.bytesToHexString(bytes));
            System.out.println("pubkey: " + Utils.bytesToHexString(nodeId.exportPublic()));
            System.out.println("signature: " + Utils.bytesToHexString(signature));
            System.out.println("");


            assertTrue(nodeId.verify(bytes, signature));
        }
    }
}