package im.redpanda;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import im.redpanda.flaschenpost.GMEchoTest;
import im.redpanda.flaschenpost.GMType;
import im.redpanda.flaschenpost.GarlicMessage;
import java.security.Security;
import org.junit.Test;

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

    NodeId nodeId = new NodeId();

    GarlicMessage garlicMessage = new GarlicMessage(new ServerContext(), nodeId);

    garlicMessage.getContent();
  }
}
