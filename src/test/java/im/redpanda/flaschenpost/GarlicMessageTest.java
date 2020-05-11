package im.redpanda.flaschenpost;

import im.redpanda.core.NodeId;
import im.redpanda.core.Server;
import im.redpanda.crypt.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.Security;

import static org.junit.Assert.*;

public class GarlicMessageTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        Server.nodeId = new NodeId();
    }

    @Test
    public void simpleCreationTest() {

        //lets target to ourselves without the private key!
        NodeId targetId = NodeId.importPublic(Server.nodeId.exportPublic());

        GMAck gmAck = new GMAck(123);

        GarlicMessage garlicMessage = new GarlicMessage(targetId);
        garlicMessage.addGMContent(gmAck);

        byte[] content = garlicMessage.getContent();

        System.out.println(content);

        System.out.println("GM: " + Utils.bytesToHexString(content));

        GMContent parse = GMParser.parse(ByteBuffer.wrap(content));

        assertNotNull(parse);

        assertEquals(parse.getClass(), GarlicMessage.class);


    }


}