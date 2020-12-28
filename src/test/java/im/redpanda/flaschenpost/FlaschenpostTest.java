package im.redpanda.flaschenpost;

import im.redpanda.core.NodeId;
import im.redpanda.core.Server;
import org.junit.Test;

import java.security.Security;

import static org.junit.Assert.*;

public class FlaschenpostTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        Server.nodeId = new NodeId();
    }

    @Test
    public void simpleTargetTest() {
        NodeId targetId = NodeId.importPublic(Server.nodeId.exportPublic());

        GMAck gmAck = new GMAck();

        GarlicMessage garlicMessage = new GarlicMessage(targetId);

        assertEquals(targetId.getKademliaId(), garlicMessage.destination);
    }


}