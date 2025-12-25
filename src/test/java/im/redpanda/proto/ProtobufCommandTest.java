package im.redpanda.proto;

import com.google.protobuf.ByteString;
import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ProtobufCommandTest {

    @Test
    public void testKademliaIdSerialization() {
        KademliaId originalId = new KademliaId();
        byte[] originalBytes = originalId.getBytes();

        KademliaIdProto proto = KademliaIdProto.newBuilder()
                .setKeyBytes(ByteString.copyFrom(originalBytes))
                .build();

        byte[] serialized = proto.toByteArray();

        try {
            KademliaIdProto parsed = KademliaIdProto.parseFrom(serialized);
            assertArrayEquals(originalBytes, parsed.getKeyBytes().toByteArray());

            KademliaId reconstructed = new KademliaId(parsed.getKeyBytes().toByteArray());
            assertEquals(originalId, reconstructed);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSendPeerListSerialization() {
        NodeId nodeId = new NodeId();
        PeerInfoProto peer = PeerInfoProto.newBuilder()
                .setIp("127.0.0.1")
                .setPort(1234)
                .setNodeId(NodeIdProto.newBuilder()
                        .setPublicKeyBytes(ByteString.copyFrom(nodeId.exportPublic()))
                        .build())
                .build();

        SendPeerList sendPeerList = SendPeerList.newBuilder()
                .addPeers(peer)
                .build();

        byte[] data = sendPeerList.toByteArray();

        try {
            SendPeerList parsed = SendPeerList.parseFrom(data);
            assertEquals(1, parsed.getPeersCount());
            PeerInfoProto parsedPeer = parsed.getPeers(0);
            assertEquals("127.0.0.1", parsedPeer.getIp());
            assertEquals(1234, parsedPeer.getPort());
            assertArrayEquals(nodeId.exportPublic(), parsedPeer.getNodeId().getPublicKeyBytes().toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
