package im.redpanda.core;

import com.google.protobuf.ByteString;
import im.redpanda.proto.NodeIdProto;
import im.redpanda.proto.PeerInfoProto;
import im.redpanda.proto.SendPeerList;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.Security;

import static org.assertj.core.api.Assertions.assertThat;

public class InboundProtobufCommandTest {

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        ByteBufferPool.init();
    }

    @Test
    public void testProtobufSend_PEERLIST() throws Exception {
        // Prepare a Protobuf SendPeerList
        NodeId nodeId = new NodeId();
        PeerInfoProto peerProto = PeerInfoProto.newBuilder()
                .setIp("1.2.3.4")
                .setPort(9999)
                .setNodeId(NodeIdProto.newBuilder()
                        .setPublicKeyBytes(ByteString.copyFrom(nodeId.exportPublic()))
                        .build())
                .build();
        SendPeerList sendPeerList = SendPeerList.newBuilder()
                .addPeers(peerProto)
                .build();

        byte[] payload = sendPeerList.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + payload.length);
        buffer.put(Command.SEND_PEERLIST);
        buffer.putInt(payload.length);
        buffer.put(payload);
        buffer.flip();

        ServerContext serverContext = ServerContext.buildDefaultServerContext();
        InboundCommandProcessor processor = new InboundCommandProcessor(serverContext);
        Peer me = new Peer("me", 1);
        me.setConnected(true);
        me.writeBuffer = ByteBuffer.allocate(1024);

        // This is expected to fail or throw exception currently
        processor.parseCommand(buffer.get(), buffer, me);

        assertThat(serverContext.getPeerList().getPeerArrayList())
                .filteredOn(p -> p.ip.equals("1.2.3.4"))
                .isNotEmpty();
    }
}
