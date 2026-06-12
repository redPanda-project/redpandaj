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

  @Test
  public void tamperedCiphertextFailsWithAeadBadTagException() {
    NodeId targetId = NodeId.importPublic(serverContext.getNodeId().exportPublic());

    GarlicMessage garlicMessage = new GarlicMessage(serverContext, targetId);
    garlicMessage.addGMContent(new GMAck(789));

    byte[] content = garlicMessage.getContent();
    // flip a bit inside the ciphertext (last byte is part of the GCM tag/ciphertext)
    content[content.length - 1] ^= 0x01;

    GarlicMessage tampered = new GarlicMessage(serverContext, content);
    assertTrue(tampered.isTargetedToUs());

    assertThrows(javax.crypto.AEADBadTagException.class, () -> tampered.decryptPayload());

    // the high-level parse path must drop the payload without content and without throwing
    tampered.parseContent();
    assertTrue(tampered.getGMContent().isEmpty());
  }

  @Test
  public void aadBindsCiphertextToDestination() {
    NodeId targetId = NodeId.importPublic(serverContext.getNodeId().exportPublic());

    GarlicMessage garlicMessage = new GarlicMessage(serverContext, targetId);
    garlicMessage.addGMContent(new GMAck(101));

    byte[] content = garlicMessage.getContent();

    // rewrite the destination to our own KademliaId is a no-op; instead simulate a relabelled
    // packet: change one byte of the destination so the AAD no longer matches
    int destinationOffset = 1 + 4; // version + totalLen
    content[destinationOffset] ^= 0x01;

    GarlicMessage relabelled = new GarlicMessage(serverContext, content);
    // if the manipulated destination happens to match us (it does not), decryption would still
    // fail because the AAD differs from the one used at encryption time
    assertThrows(java.security.GeneralSecurityException.class, () -> relabelled.decryptPayload());
  }
}
