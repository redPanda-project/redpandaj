package im.redpanda.flaschenpost;

import static org.junit.Assert.*;

import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import java.security.Security;
import org.junit.Test;

public class GarlicMessageTest {

  private static final ServerContext serverContext;

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    serverContext = ServerContext.buildDefaultServerContext();
  }

  @Test
  public void simpleCreationTest() {

    // lets target to ourselves without the private key!
    NodeId targetId = NodeId.importPublic(serverContext.getNodeId().exportPublic());

    GMAck gmAck = new GMAck(123);

    GarlicMessage garlicMessage = new GarlicMessage(serverContext, targetId);
    garlicMessage.addGMContent(gmAck);

    byte[] content = garlicMessage.getContent();

    GMContent parse = GMParser.parse(serverContext, content);

    assertNotNull(parse);

    assertEquals(parse.getClass(), GarlicMessage.class);
  }

  @Test
  public void parseTestAckGarlicMessage() {

    // lets target to ourselves without the private key!
    NodeId targetId = NodeId.importPublic(serverContext.getNodeId().exportPublic());

    GMAck gmAck = new GMAck(456);

    GarlicMessage garlicMessage = new GarlicMessage(serverContext, targetId);
    garlicMessage.addGMContent(gmAck);

    byte[] content = garlicMessage.getContent();

    GMContent parse = GMParser.parse(serverContext, content);

    assertNotNull(parse);

    assertEquals(parse.getClass(), GarlicMessage.class);

    GarlicMessage parsedGM = (GarlicMessage) parse;

    assertEquals(serverContext.getNodeId().getKademliaId(), parsedGM.destination);

    assertEquals(true, parsedGM.isTargetedToUs());

    assertEquals(1, parsedGM.getGMContent().size());

    GMContent gmContent = parsedGM.getGMContent().getFirst();

    assertEquals(gmContent.getClass(), GMAck.class);

    GMAck parsedAck = (GMAck) gmContent;

    assertEquals(456, parsedAck.getAckid());
  }

  @Test
  public void parseTestRandomDestinationGarlicMessage() {
    // lets target to a random node id!
    NodeId targetId = NodeId.importPublic(new NodeId().exportPublic());

    GMAck gmAck = new GMAck(456);

    GarlicMessage garlicMessage = new GarlicMessage(serverContext, targetId);
    garlicMessage.addGMContent(gmAck);

    byte[] content = garlicMessage.getContent();

    assertEquals(GMType.GARLIC_MESSAGE.getId(), content[0]);

    GarlicMessage parsedGM = (GarlicMessage) GMParser.parse(serverContext, content);

    assertEquals(GarlicMessage.class, parsedGM.getClass());

    assertEquals(targetId.getKademliaId(), parsedGM.destination);

    assertEquals(false, parsedGM.isTargetedToUs());

    assertEquals(targetId.getKademliaId(), parsedGM.destination);

    assertThrows(RuntimeException.class, () -> parsedGM.parseContent());
  }

  @Test
  public void parseTestSecondParse() {
    // lets target to a random node id!
    NodeId targetId = NodeId.importPublic(new NodeId().exportPublic());

    GMAck gmAck = new GMAck(456);

    GarlicMessage garlicMessage = new GarlicMessage(serverContext, targetId);
    garlicMessage.addGMContent(gmAck);

    byte[] content = garlicMessage.getContent();

    GMParser.parse(serverContext, content);

    GarlicMessage parsedAgainGarlicMessage = (GarlicMessage) GMParser.parse(serverContext, content);

    assertNull(parsedAgainGarlicMessage);
  }
}
