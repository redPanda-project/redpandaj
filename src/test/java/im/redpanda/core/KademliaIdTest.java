package im.redpanda.core;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.Security;

import static org.junit.Assert.*;

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