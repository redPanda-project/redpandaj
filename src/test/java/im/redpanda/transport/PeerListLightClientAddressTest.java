package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.core.Command;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import im.redpanda.proto.PeerInfoProto;
import im.redpanda.proto.SendPeerList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * T150/TD183: port 0 is not an address, so it must not decide who owns one.
 *
 * <p>The handshake carries the sender's <em>listening</em> port and a light client has none, so it
 * announces 0 — every inbound light client from one ip therefore shares the single {@code
 * peerlistIpPort} key {@code "<ip>:0"}. Since T120a/#354 {@code PeerList.addLocked} resolves a
 * contested address by taking it away from one of the two objects, so on a loopback topology (the
 * mobile 4-node e2e, the emulator gate, two clients behind one NAT) every light client but the
 * first ended up with {@code ip == null}.
 *
 * <p>That is not cosmetic. An ip-less peer is skipped by {@code handleRequestPeerList}, and the
 * peer list it <em>asks</em> for comes back without a single local relay: {@code
 * Utils.isPlausibleAdvertisedAddress} only believes a loopback address when the asking peer is
 * itself local, and a peer with no ip is not. The observed failure was {@code Bob discovered only 0
 * of 3 relay candidates with encryption keys} in the mobile suites {@code ms05_reverse_garlic},
 * {@code ms06_two_layer_ack} and {@code ms08_group_chat} — always the second client to connect,
 * never the first.
 */
class PeerListLightClientAddressTest {

  private static final String LOOPBACK = "127.0.0.1";
  private static final List<Integer> RELAY_PORTS = List.of(50581, 50582, 50583);

  private ServerContext ctx;
  private InboundCommandProcessor proc;

  @BeforeEach
  void setup() {
    ctx = ServerContext.buildDefaultServerContext();
    ctx.setPort(50580);
    proc = new InboundCommandProcessor(ctx);
    ByteBufferPool.init();
  }

  /** An inbound light client as {@code ConnectionReaderThread.parseHandshake} builds it: port 0. */
  private static Peer lightClient(String ip) {
    Peer peer = new Peer(ip, 0);
    peer.setNodeId(NodeId.generateWithSimpleKey());
    return peer;
  }

  @Test
  void twoLightClientsFromOneIp_bothKeepTheirAddress() {
    Peer alice = lightClient(LOOPBACK);
    ctx.getPeerList().add(alice);
    alice.setConnected(true);

    Peer bob = lightClient(LOOPBACK);
    ctx.getPeerList().add(bob);

    assertThat(bob.getIp())
        .as("port 0 is not an address, so alice cannot own it and bob cannot lose his")
        .isEqualTo(LOOPBACK);
    assertThat(alice.getIp()).isEqualTo(LOOPBACK);
    assertThat(ctx.getPeerList().get(alice.getKademliaId())).isSameAs(alice);
    assertThat(ctx.getPeerList().get(bob.getKademliaId())).isSameAs(bob);
  }

  /** The same the other way round: the sitting peer must not lose its address either. */
  @Test
  void twoDisconnectedLightClientsFromOneIp_bothKeepTheirAddress() {
    Peer alice = lightClient(LOOPBACK);
    ctx.getPeerList().add(alice);

    Peer bob = lightClient(LOOPBACK);
    ctx.getPeerList().add(bob);

    assertThat(alice.getIp()).isEqualTo(LOOPBACK);
    assertThat(bob.getIp()).isEqualTo(LOOPBACK);
  }

  /**
   * The mobile e2e failure itself: the second light client asks the entry node for its peer list
   * and must get all three loopback relays, with their public and X25519 keys.
   */
  @Test
  void theSecondLightClientFromOneIp_stillGetsTheLoopbackRelaysWithTheirKeys() {
    for (int port : RELAY_PORTS) {
      ctx.getPeerList().add(new Peer(LOOPBACK, port, NodeId.generateWithSimpleKey()));
    }

    Peer alice = lightClient(LOOPBACK);
    ctx.getPeerList().add(alice);
    alice.setConnected(true);

    Peer bob = lightClient(LOOPBACK);
    ctx.getPeerList().add(bob);

    List<PeerInfoProto> advertised = requestPeerList(bob);

    List<Integer> relaysWithKeys = new ArrayList<>();
    for (PeerInfoProto entry : advertised) {
      if (!RELAY_PORTS.contains(entry.getPort())) {
        continue;
      }
      if (entry.hasNodeId() && !entry.getEncryptionPublicKey().isEmpty()) {
        relaysWithKeys.add(entry.getPort());
      }
    }

    assertThat(relaysWithKeys)
        .as("Bob must discover all 3 relay candidates with encryption keys")
        .containsExactlyInAnyOrderElementsOf(RELAY_PORTS);
  }

  private List<PeerInfoProto> requestPeerList(Peer requester) {
    requester.writeBuffer = ByteBuffer.allocate(1024 * 64);

    assertThat(proc.parseCommand(Command.REQUEST_PEERLIST, ByteBuffer.allocate(0), requester))
        .isEqualTo(1);

    requester.writeBuffer.flip();
    assertThat(requester.writeBuffer.get()).isEqualTo(Command.SEND_PEERLIST);
    byte[] payload = new byte[requester.writeBuffer.getInt()];
    requester.writeBuffer.get(payload);
    try {
      return SendPeerList.parseFrom(payload).getPeersList();
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError("peer list frame did not parse", e);
    }
  }
}
