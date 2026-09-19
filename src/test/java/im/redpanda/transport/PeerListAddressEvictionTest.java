package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * TD214 (security) and TD213 (the address a connected peer never got).
 *
 * <p><b>TD214.</b> {@code PeerList.removeIpPort(String, int)} was the last removal path that was
 * not value-checked: it took whoever the address key pointed at out of all three indices — the
 * identity map, the address map and the peer array list. Its only production caller was the
 * self-connect branch of {@link ConnectionReaderThread#parseHandshake}, which runs on the
 * <em>plaintext</em> part of a handshake: TCP establishes the remote ip, but the announced
 * listening port and the announced identity are simply whatever the far side chose to send (the
 * proof of identity is the first encrypted PING, several steps later). So a host that shares an ip
 * with a peer we know — same NAT, a co-located container, a shared exit — could have that peer
 * dropped from our peer list by echoing our own {@code KademliaId} back at us and naming the
 * victim's port. T150b narrowed the reachable set to dialable addresses; it did not close the hole.
 *
 * <p>The caller has the {@link Peer} object it dialled, so it acts on that object now and no
 * removal-by-address exists at all any more ({@link PeerList#getByAddress(String, int)} is the
 * read-only replacement). What it learned is about one address, so how much of the dialled object
 * it costs depends on what else that object is: an id-less dial target <em>is</em> the address and
 * goes entirely ({@link PeerList#removeExact(Peer)}), while a peer that carries a {@link
 * KademliaId} only loses its connection details ({@code clearConnectionDetails}) — an identity echo
 * is not evidence about the node behind that identity, and dropping its registration and its keyed
 * {@link NodeId} on it would be remotely triggerable in the same way.
 *
 * <p><b>TD213.</b> When two identities claim one dialable address and the sitting peer is live, the
 * newcomer is registered without an address — even when the newcomer is the peer whose connection
 * is being established (on the testnet: the auto-updater uploader restarting with a fresh identity,
 * "Connected successfully to null:0"). The tests at the bottom pin that it stays that way, and why:
 * a completed handshake shows we can talk to a socket, not that the <em>listening</em> port the far
 * side announced belongs to it, and the identity on a connection is not proven at all — {@code
 * ACTIVATE_ENCRYPTION} carries a bare, unsigned ephemeral X25519 key and the session secret is
 * ephemeral-to-ephemeral, with the public static verify keys used only as HKDF salt. So anyone who
 * knows a node's public key can complete a handshake as that node, and filling an address onto a
 * registered identity on that evidence would hand an attacker an address that then spreads through
 * {@code PeerExchangeHandler}, is persisted by {@code Saver} and dialled by {@code
 * OutboundHandler}. Repairing the lost address belongs behind a real proof of identity (TD178).
 */
class PeerListAddressEvictionTest {

  /** The ip the attacker and the victim share: one NAT, one container host, one exit node. */
  private static final String SHARED_IP = "198.51.100.9";

  private static final int VICTIM_PORT = 59558;
  private static final int ATTACKER_PORT = 40001;

  private ServerContext ctx;
  private PeerList peerList;

  @BeforeEach
  void setup() {
    ctx = ServerContext.buildDefaultServerContext();
    peerList = ctx.getPeerList();
    ByteBufferPool.init();
  }

  /** A 30-byte v23 handshake: magic, version, client type, announced identity, announced port. */
  private static ByteBuffer handshake(byte[] identity, int announcedPort) {
    ByteBuffer handshake = ByteBuffer.allocate(30);
    handshake.put(Server.MAGIC.getBytes());
    handshake.put((byte) Server.VERSION);
    handshake.put((byte) 0);
    handshake.put(identity);
    handshake.putInt(announcedPort);
    handshake.flip();
    return handshake;
  }

  // ---------------------------------------------------------------------------------------------
  // TD214: nobody may be evicted by naming an address.
  // ---------------------------------------------------------------------------------------------

  /**
   * The security claim. We dial a host; it answers with <em>our own</em> identity (public
   * information) and announces the listening port of a peer that happens to share its ip. The
   * self-connect branch fires, and the victim must survive it in all three indices.
   *
   * <p>Negative control: with {@code removeIpPort(SHARED_IP, VICTIM_PORT)} in place of {@code
   * removeExact}, every assertion about the victim below fails — it is gone from the identity map,
   * from the address map and from the list, while its {@link Peer} object still believes it owns
   * the address.
   */
  @Test
  void aWrongIdentityAnnouncingAKnownAddressCannotEvictTheSittingPeer() throws Exception {
    NodeId victimId = NodeId.generateWithSimpleKey();
    Peer victim = new Peer(SHARED_IP, VICTIM_PORT, victimId);
    victim.setConnected(true);
    peerList.add(victim);

    // The peer we dialled: the attacker's own address, a different port behind the same ip.
    Peer dialled = new Peer(SHARED_IP, ATTACKER_PORT);
    peerList.add(dialled);

    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake outbound = new PeerInHandshake(SHARED_IP, dialled, channel);

      boolean accepted =
          ConnectionReaderThread.parseHandshake(
              ctx, outbound, handshake(ctx.getOwnNodeId().getBytes(), VICTIM_PORT));

      assertThat(accepted).as("a self-connect is never a usable handshake").isFalse();
      assertThat(outbound.getStatus()).as("status 2 is the disconnect code").isEqualTo(2);
      assertThat(channel.isOpen()).isFalse();
    }

    assertThat(peerList.get(victimId.getKademliaId()))
        .as("the victim must still be registered under its identity")
        .isSameAs(victim);
    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT))
        .as("and must still own its address")
        .isSameAs(victim);
    assertThat(peerList.snapshot()).as("and must still be in the peer list").contains(victim);
    assertThat(victim.getIp()).isEqualTo(SHARED_IP);
    assertThat(victim.getPort()).isEqualTo(VICTIM_PORT);

    // What the branch is actually for: the address we dialled is ours, so stop dialling it.
    assertThat(peerList.snapshot())
        .as("the peer we dialled is the one that goes")
        .doesNotContain(dialled);
    assertThat(peerList.getByAddress(SHARED_IP, ATTACKER_PORT)).isNull();
  }

  /**
   * The same branch with the announced port left at 0, the shape an inbound light client produces:
   * still nothing may be evicted, and the dialled peer still goes.
   */
  @Test
  void aSelfConnectWithoutAnAnnouncedPortEvictsNobodyElseEither() throws Exception {
    NodeId victimId = NodeId.generateWithSimpleKey();
    Peer victim = new Peer(SHARED_IP, VICTIM_PORT, victimId);
    peerList.add(victim);

    Peer dialled = new Peer(SHARED_IP, ATTACKER_PORT);
    peerList.add(dialled);

    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake outbound = new PeerInHandshake(SHARED_IP, dialled, channel);
      ConnectionReaderThread.parseHandshake(
          ctx, outbound, handshake(ctx.getOwnNodeId().getBytes(), 0));
    }

    assertThat(peerList.get(victimId.getKademliaId())).isSameAs(victim);
    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT)).isSameAs(victim);
    assertThat(peerList.snapshot()).doesNotContain(dialled);
  }

  /**
   * The dialled object carries an identity: only its address goes, the registration and the keyed
   * {@link NodeId} survive. Full removal on an unauthenticated echo of our public id would be a
   * remotely triggerable way to un-register a node we track.
   */
  @Test
  void aSelfConnectThroughAnIdentifiedPeerOnlyCostsThatPeerItsAddress() throws Exception {
    NodeId dialledId = NodeId.generateWithSimpleKey();
    Peer dialled = new Peer(SHARED_IP, ATTACKER_PORT, dialledId);
    peerList.add(dialled);

    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake outbound = new PeerInHandshake(SHARED_IP, dialled, channel);
      ConnectionReaderThread.parseHandshake(
          ctx, outbound, handshake(ctx.getOwnNodeId().getBytes(), ATTACKER_PORT));
    }

    assertThat(peerList.get(dialledId.getKademliaId()))
        .as("the peer stays registered under its identity")
        .isSameAs(dialled);
    assertThat(dialled.getNodeId().hasKey()).as("and keeps its keys").isTrue();
    assertThat(peerList.snapshot()).contains(dialled);
    assertThat(dialled.getIp()).as("but the address is gone").isNull();
    assertThat(peerList.getByAddress(SHARED_IP, ATTACKER_PORT)).isNull();
  }

  /**
   * An inbound self-connect has no {@link Peer} at all ({@code PeerInHandshake.getPeer() == null}),
   * so nothing is removed — unchanged, and pinned here because the removal is now driven by that
   * object rather than by an address.
   */
  @Test
  void anInboundSelfConnectRemovesNothing() throws Exception {
    NodeId victimId = NodeId.generateWithSimpleKey();
    Peer victim = new Peer(SHARED_IP, VICTIM_PORT, victimId);
    peerList.add(victim);
    int sizeBefore = peerList.size();

    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake inbound = new PeerInHandshake(SHARED_IP, channel);
      ConnectionReaderThread.parseHandshake(
          ctx, inbound, handshake(ctx.getOwnNodeId().getBytes(), VICTIM_PORT));
    }

    assertThat(peerList.size()).isEqualTo(sizeBefore);
    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT)).isSameAs(victim);
  }

  /** {@link PeerList#getByAddress} is an observation, not a mutation. */
  @Test
  void getByAddressDoesNotRemoveAnything() {
    Peer peer = new Peer(SHARED_IP, VICTIM_PORT, NodeId.generateWithSimpleKey());
    peerList.add(peer);

    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT)).isSameAs(peer);
    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT))
        .as("and again — it must still be there")
        .isSameAs(peer);
    assertThat(peerList.snapshot()).contains(peer);
  }

  // ---------------------------------------------------------------------------------------------
  // TD213: the address a connected peer does not get, and why it stays that way.
  // ---------------------------------------------------------------------------------------------

  /**
   * A live sitting peer keeps its address even against the peer whose connection is being
   * established, and the newcomer is registered without one. This is the guard that keeps TD214's
   * eviction primitive from coming back in through the front door.
   *
   * <p>TD213 asked for the newcomer — the peer we are demonstrably talking to — to keep or regain
   * the address. It must not, and the reason is the same missing proof twice over: a completed
   * handshake shows we can talk to a socket, not that the <em>listening</em> port the far side
   * announced belongs to it; and the identity on a connection is not proven at all, because {@code
   * ACTIVATE_ENCRYPTION} carries a bare, unsigned ephemeral X25519 key and the session secret is
   * ephemeral-to-ephemeral (the static verify keys are HKDF salt, and they are public). Filling an
   * address in on that evidence would hand an attacker the address of an identity it does not own,
   * which then spreads through {@code PeerExchangeHandler}, is persisted by {@code Saver} and
   * dialled by {@code OutboundHandler}. The repair belongs behind a real proof of identity (TD178).
   */
  @Test
  void aLiveSittingPeerKeepsItsAddressAgainstAnEstablishedConnection() {
    NodeId sittingId = NodeId.generateWithSimpleKey();
    Peer sitting = new Peer(SHARED_IP, VICTIM_PORT, sittingId);
    sitting.setConnected(true);
    peerList.add(sitting);

    NodeId newcomerId = NodeId.generateWithSimpleKey();
    Peer newcomer = new Peer(SHARED_IP, VICTIM_PORT, newcomerId);
    assertThat(peerList.add(newcomer, 1_000))
        .as("a fresh identity is registered as itself")
        .isNull();

    assertThat(sitting.getIp()).isEqualTo(SHARED_IP);
    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT)).isSameAs(sitting);
    assertThat(newcomer.getIp()).as("the newcomer gives the address up").isNull();
    assertThat(peerList.get(newcomerId.getKademliaId()))
        .as("but it is registered, and it owns the connection")
        .isSameAs(newcomer);
  }

  /**
   * The same for a dial in flight: an address is not taken out from under a running connection
   * attempt either.
   */
  @Test
  void aPeerBeingDialledKeepsItsAddress() {
    Peer dialling = new Peer(SHARED_IP, VICTIM_PORT, NodeId.generateWithSimpleKey());
    dialling.isConnecting = true;
    peerList.add(dialling);

    Peer newcomer = new Peer(SHARED_IP, VICTIM_PORT, NodeId.generateWithSimpleKey());
    peerList.add(newcomer, 1_000);

    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT)).isSameAs(dialling);
    assertThat(newcomer.getIp()).isNull();
  }

  /**
   * A registered, address-less peer stays address-less: nothing fills an address in from a
   * completed handshake, so no identity can be pointed at an address it did not prove (TD178).
   */
  @Test
  void anAddressLessPeerIsNotGivenAnAddressByAConnection() {
    NodeId id = NodeId.generateWithSimpleKey();
    Peer addressLess = new Peer(null, 0, id);
    peerList.add(addressLess);

    // What a handshake from that identity looks like to the peer list: same object, and the
    // announced address only exists on the PeerInHandshake.
    assertThat(peerList.add(addressLess, 1_000)).isSameAs(addressLess);

    assertThat(addressLess.getIp()).isNull();
    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT)).isNull();
  }
}
