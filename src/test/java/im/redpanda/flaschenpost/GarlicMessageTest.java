package im.redpanda.flaschenpost;

import im.redpanda.core.NodeId;
import im.redpanda.crypt.Utils;
import org.junit.Test;

import java.security.Security;

import static org.junit.Assert.*;

public class GarlicMessageTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void simpleCreationTest() {

        NodeId targetId = new NodeId();

        GMAck gmAck = new GMAck(123);

        GarlicMessage garlicMessage = new GarlicMessage(targetId);
        garlicMessage.addGMContent(gmAck);

        byte[] content = garlicMessage.getContent();

        System.out.println(content);

        System.out.println("GM: " + Utils.bytesToHexString(content));

        GMParser.parse(content);


    }


}