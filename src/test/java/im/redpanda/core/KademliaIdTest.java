package im.redpanda.core;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.security.Security;
import org.junit.Test;

public class KademliaIdTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void testEquals() {

    NodeId nodeId = new NodeId();

    KademliaId kademliaId = nodeId.getKademliaId();

    KademliaId clonedByBytes = KademliaId.fromBuffer(ByteBuffer.wrap(kademliaId.getBytes()));

    assertEquals(kademliaId, clonedByBytes);

    assertTrue(kademliaId.equals(clonedByBytes));
  }
}
