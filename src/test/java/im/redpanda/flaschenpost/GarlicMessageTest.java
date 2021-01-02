package im.redpanda.flaschenpost;

import im.redpanda.core.NodeId;
import im.redpanda.core.Server;
import org.junit.Test;

import java.security.Security;

import static org.junit.Assert.*;

public class GarlicMessageTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        Server.nodeId = new NodeId();
        Server.NONCE = Server.nodeId.getKademliaId();
    }

    @Test
    public void simpleCreationTest() {

        //lets target to ourselves without the private key!
        NodeId targetId = NodeId.importPublic(Server.nodeId.exportPublic());

        GMAck gmAck = new GMAck(123);

        GarlicMessage garlicMessage = new GarlicMessage(targetId);
        garlicMessage.addGMContent(gmAck);

        byte[] content = garlicMessage.getContent();

        GMContent parse = GMParser.parse(content);

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

        GMContent parse = GMParser.parse(content);

        assertNotNull(parse);

        assertEquals(parse.getClass(), GarlicMessage.class);

        GarlicMessage parsedGM = (GarlicMessage) parse;

        assertEquals(Server.nodeId.getKademliaId(), parsedGM.destination);

        assertEquals(true, parsedGM.isTargetedToUs());

        assertEquals(1, parsedGM.getGMContent().size());

        GMContent gmContent = parsedGM.getGMContent().get(0);

        assertEquals(gmContent.getClass(), GMAck.class);

        GMAck parsedAck = (GMAck) gmContent;

        assertEquals(456, parsedAck.getAckid());


    }

    @Test
    public void parseTestRandomDestinationGarlicMessage() {
        //lets target to a random node id!
        NodeId targetId = NodeId.importPublic(new NodeId().exportPublic());

        GMAck gmAck = new GMAck(456);

        GarlicMessage garlicMessage = new GarlicMessage(targetId);
        garlicMessage.addGMContent(gmAck);

        byte[] content = garlicMessage.getContent();

        assertEquals(GMType.GARLIC_MESSAGE.getId(), content[0]);

        GMContent parse = GMParser.parse(content);

        assertEquals(GarlicMessage.class, parse.getClass());

        GarlicMessage parsedGM = (GarlicMessage) GMParser.parse(content);

        assertEquals(targetId.getKademliaId(), parsedGM.destination);

        assertEquals(false, parsedGM.isTargetedToUs());

        assertEquals(targetId.getKademliaId(), parsedGM.destination);

        assertThrows(RuntimeException.class, () -> parsedGM.parseContent());
    }


}