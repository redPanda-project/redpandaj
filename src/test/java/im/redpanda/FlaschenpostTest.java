package im.redpanda;

import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import im.redpanda.flaschenpost.GMEchoTest;
import im.redpanda.flaschenpost.GMType;
import im.redpanda.flaschenpost.GarlicMessage;
import org.junit.Test;

import java.security.Security;

import static org.assertj.core.api.Assertions.assertThat;

public class FlaschenpostTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @Test
    public void createGMEchoTest() {
        GMEchoTest gmEchoTest = new GMEchoTest();
        byte[] content = gmEchoTest.getContent();

        assertThat(content).isNotNull();
        assertThat(content[0]).isEqualTo(GMType.ECHO.getId());
    }


    @Test
    public void echoNestedGM() {
        GMEchoTest gmEchoTest = new GMEchoTest();

        NodeId nodeId = new NodeId();

        GarlicMessage garlicMessage = new GarlicMessage(new ServerContext(), nodeId);

        byte[] content = garlicMessage.getContent();


    }

}
