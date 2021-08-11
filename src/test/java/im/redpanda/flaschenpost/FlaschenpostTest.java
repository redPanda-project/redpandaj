package im.redpanda.flaschenpost;

import im.redpanda.core.NodeId;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import org.junit.Test;

import java.security.Security;

import static org.junit.Assert.assertEquals;

public class FlaschenpostTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        Server.nodeId = new NodeId();
    }

    @Test
    public void simpleTargetTest() {
        ServerContext serverContext = new ServerContext();

        NodeId targetId = NodeId.importPublic(Server.nodeId.exportPublic());

        GMAck gmAck = new GMAck();

        GarlicMessage garlicMessage = new GarlicMessage(serverContext, targetId);

        assertEquals(targetId.getKademliaId(), garlicMessage.destination);
    }


}