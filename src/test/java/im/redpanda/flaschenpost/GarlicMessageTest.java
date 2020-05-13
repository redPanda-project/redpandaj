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

        GMContent parse = GMParser.parse(ByteBuffer.wrap(content));

        assertNotNull(parse);

        assertEquals(parse.getClass(), GarlicMessage.class);


    }

    @Test
    public void parseTestAckGarlicMessage() {

        //lets target to ourselves without the private key!
        NodeId targetId = NodeId.importPublic(Server.nodeId.exportPublic());

        GMAck gmAck = new GMAck(456);

        GarlicMessage garlicMessage = new GarlicMessage(targetId);
        garlicMessage.addGMContent(gmAck);

        byte[] content = garlicMessage.getContent();

        GMContent parse = GMParser.parse(ByteBuffer.wrap(content));

        assertNotNull(parse);

        assertEquals(parse.getClass(), GarlicMessage.class);

        GarlicMessage parsedGM = (GarlicMessage) parse;

        assertEquals(1, parsedGM.getGMContent().size());

        GMContent gmContent = parsedGM.getGMContent().get(0);

        assertEquals(gmContent.getClass(), GMAck.class);

        GMAck parsedAck = (GMAck) gmContent;

        assertEquals(456, parsedAck.getAckid());


    }


}