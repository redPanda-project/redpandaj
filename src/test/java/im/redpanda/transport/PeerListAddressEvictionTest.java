package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
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
 * <p>The caller has the {@link Peer} object it dialled and that object is the only thing it wants
 * gone, so it uses {@link PeerList#removeExact(Peer)} now, and no removal-by-address exists at all
 * any more ({@link PeerList#getByAddress(String, int)} is the read-only replacement).
 *
 * <p><b>TD213.</b> When two identities claim one dialable address and the sitting peer is live, the
 * newcomer is registered without an address — even when the newcomer is the peer whose connection
 * is being established (on the testnet: the auto-updater uploader restarting with a fresh identity,
 * "Connected successfully to null:0"). It still is, deliberately: a completed handshake proves we
 * can talk to a socket, not that the listening port the far side announced belongs to it, and
 * handing the address over on that evidence would recreate the very eviction primitive TD214
 * removes, for co-located hosts. What was wrong is that the address was then lost <em>forever</em>,
 * because nothing in this class ever filled an address back onto an already registered peer. It is
 * filled in now, on the next completed handshake after the address stops being owned.
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
  // TD213: the address a connected peer never got.
  // ---------------------------------------------------------------------------------------------

  /**
   * A live sitting peer keeps its address even against the peer whose connection is being
   * established. This is the guard that keeps TD214's eviction primitive from coming back in
   * through the front door: the announced listening port is not proven by the connection.
   */
  @Test
  void aLiveSittingPeerKeepsItsAddressAgainstAnEstablishedConnection() {
    NodeId sittingId = NodeId.generateWithSimpleKey();
    Peer sitting = new Peer(SHARED_IP, VICTIM_PORT, sittingId);
    sitting.setConnected(true);
    peerList.add(sitting);

    NodeId newcomerId = NodeId.generateWithSimpleKey();
    Peer newcomer = new Peer(SHARED_IP, VICTIM_PORT, newcomerId);
    assertThat(peerList.addFromCompletedHandshake(newcomer, SHARED_IP, VICTIM_PORT, 1_000))
        .as("a fresh identity is registered as itself")
        .isNull();

    assertThat(sitting.getIp()).isEqualTo(SHARED_IP);
    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT)).isSameAs(sitting);
    assertThat(newcomer.getIp()).as("the newcomer gives the address up, as before").isNull();
    assertThat(peerList.get(newcomerId.getKademliaId()))
        .as("but it is registered, and it owns the connection")
        .isSameAs(newcomer);
  }

  /**
   * TD213 itself: once the stale peer is gone, the next completed handshake gives the peer we are
   * talking to its address back. It used to stay {@code null:0} for the lifetime of the object,
   * because nothing ever filled an address onto an already registered peer.
   *
   * <p>The second handshake continues on the <em>registered</em> object, which is exactly why the
   * announced address has to be passed alongside it: {@code ConnectionReaderThread.parseHandshake}
   * resolves a known identity through {@code PeerList.get(KademliaId)}, and that object is the one
   * whose ip is null.
   */
  @Test
  void afterTheStalePeerIsGoneTheEstablishedConnectionGetsItsAddress() {
    Peer stale = new Peer(SHARED_IP, VICTIM_PORT, NodeId.generateWithSimpleKey());
    stale.setConnected(true);
    peerList.add(stale);

    NodeId uploaderId = NodeId.generateWithSimpleKey();
    Peer uploader = new Peer(SHARED_IP, VICTIM_PORT, uploaderId);
    peerList.addFromCompletedHandshake(uploader, SHARED_IP, VICTIM_PORT, 1_000);
    assertThat(uploader.getIp()).as("lost the contested address, as before").isNull();

    // The stale peer is reaped (PeerJobs / a disconnect that removes it).
    assertThat(peerList.remove(stale)).isTrue();
    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT)).isNull();

    // The uploader reconnects. parseHandshake resolves the identity and continues on the
    // registered, address-less object; the announced address comes from the handshake.
    assertThat(peerList.addFromCompletedHandshake(uploader, SHARED_IP, VICTIM_PORT, 1_000))
        .as("the registered object answers for its own identity")
        .isSameAs(uploader);

    assertThat(uploader.getIp()).isEqualTo(SHARED_IP);
    assertThat(uploader.getPort()).isEqualTo(VICTIM_PORT);
    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT))
        .as("and it is the registered owner of the address")
        .isSameAs(uploader);
    assertThat(peerList.snapshot()).containsOnlyOnce(uploader);
  }

  /**
   * The adoption fills a gap and nothing more: an address another peer owns is never taken, not
   * even by the peer whose connection just completed. Without this guard TD214's eviction is back,
   * for every host that shares an ip with a peer we know.
   */
  @Test
  void anEstablishedConnectionNeverTakesAnAddressSomebodyElseOwns() {
    NodeId ownerId = NodeId.generateWithSimpleKey();
    Peer owner = new Peer(SHARED_IP, VICTIM_PORT, ownerId);
    peerList.add(owner);

    // A peer of ours with no address at all — the state clearConnectionDetails leaves behind.
    NodeId claimantId = NodeId.generateWithSimpleKey();
    Peer claimant = new Peer(null, 0, claimantId);
    peerList.add(claimant);

    peerList.addFromCompletedHandshake(claimant, SHARED_IP, VICTIM_PORT, 1_000);

    assertThat(claimant.getIp()).as("the address is owned, so it is not adopted").isNull();
    assertThat(owner.getIp()).isEqualTo(SHARED_IP);
    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT)).isSameAs(owner);
  }

  /**
   * And it never overwrites an address a peer already has. The announced address is unproven (the
   * identity comes from the plaintext handshake), so refreshing a good address with it would let
   * anyone we talk to make another node undialable for us — that move belongs behind the first
   * encrypted PING and is tracked separately (TD178).
   */
  @Test
  void anEstablishedConnectionNeverOverwritesAnAddressItAlreadyHas() {
    NodeId id = NodeId.generateWithSimpleKey();
    Peer peer = new Peer("203.0.113.7", 59558, id);
    peerList.add(peer);

    peerList.addFromCompletedHandshake(peer, SHARED_IP, VICTIM_PORT, 1_000);

    assertThat(peer.getIp()).isEqualTo("203.0.113.7");
    assertThat(peer.getPort()).isEqualTo(59558);
    assertThat(peerList.getByAddress("203.0.113.7", 59558)).isSameAs(peer);
    assertThat(peerList.getByAddress(SHARED_IP, VICTIM_PORT))
        .as("the announced address is not keyed for it either")
        .isNull();
  }

  /**
   * An undialable announced address is not adopted: port 0 is the <em>listening</em> port a light
   * client does not have, so it would leave the peer addressed but still undialable, in the shared
   * {@code "<ip>:0"} bucket (T150/TD183).
   */
  @Test
  void anUndialableAnnouncedAddressIsNotAdopted() {
    NodeId id = NodeId.generateWithSimpleKey();
    Peer lightClient = new Peer(null, 0, id);
    peerList.add(lightClient);

    peerList.addFromCompletedHandshake(lightClient, SHARED_IP, 0, 1_000);

    assertThat(lightClient.getIp()).isNull();
    assertThat(peerList.getByAddress(SHARED_IP, 0)).isNull();
  }
}
