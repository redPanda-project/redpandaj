package im.redpanda;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import im.redpanda.routing.GMEchoTest;
import im.redpanda.routing.GMType;
import im.redpanda.routing.GarlicMessage;
import java.security.Security;
import org.junit.jupiter.api.Test;

class FlaschenpostTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  void createGMEchoTest() {
    GMEchoTest gmEchoTest = new GMEchoTest();
    byte[] content = gmEchoTest.getContent();

    assertThat(content).isNotNull();
    assertThat(content[0]).isEqualTo(GMType.ECHO.getId());
  }

  @Test
  void echoNestedGM() {

    NodeId nodeId = new NodeId();

    GarlicMessage garlicMessage = new GarlicMessage(new ServerContext(), nodeId);

    garlicMessage.getContent();
  }
}
