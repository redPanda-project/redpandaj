package im.redpanda;

import im.redpanda.core.NodeId;
import im.redpanda.flaschenpost.GMEchoTest;
import im.redpanda.flaschenpost.GMType;
import im.redpanda.flaschenpost.GarlicMessage;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;

public class FlaschenpostTest {


    @Test
    public void createGMEchoTest() {
        GMEchoTest gmEchoTest = new GMEchoTest();
        byte[] content = gmEchoTest.getContent();

        Assert.assertNotNull(content);
        Assert.assertThat(content[0], is(GMType.ECHO.getId()));
    }


    @Test
    public void echoNesteGM() {
        GMEchoTest gmEchoTest = new GMEchoTest();

        NodeId nodeId = new NodeId();

        GarlicMessage garlicMessage = new GarlicMessage(nodeId);

        byte[] content = garlicMessage.getContent();




    }

}
