package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * T120 — one {@link Peer} object per node identity (TD162), no public key lost on the way there
 * (TD164), no peer left connected behind a cancelled selection key (TD165), no dial into the window
 * in which a peer swaps a connection in (TD143).
 *
 * <p>Background: the peer list could hold two objects for one node — an id-less seed/restored entry
 * and a handshake-built one sharing an address, or an identity moved onto a second object by {@code
 * updateKademliaId}. Both are dial candidates for {@link OutboundHandler}, only one is reachable by
 * identity, and dialling through the unregistered one drove the node1&lt;-&gt;node2 redial loop of
 * 2026-09-03 (TD142). #339 made that self-healing on the next completed handshake; these tests pin
 * that the second object is not created in the first place.
 */
class PeerIdentityInvariantTest {

  static {
    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /**
   * The seed case: an id-less entry for an address exists (reseed, restored {@code PeerSaveable},
   * gossip) and the node behind it identifies itself. The identity must land on that very object,
   * not on a second one.
   */
  @Test
  void addingAnIdentifiedPeerForAKnownAddressKeepsOneObject() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();

    Peer seed = new Peer("10.0.0.2", 59558);
    peerList.add(seed);

    NodeId identity = NodeId.generateWithSimpleKey();
    Peer identified = new Peer("10.0.0.2", 59558, identity);
    Peer owner = peerList.add(identified);

    assertThat(owner).as("the address already had an owner, it must be handed back").isSameAs(seed);
    assertThat(peerList.snapshot())
        .as("a second Peer object for one node is what the outbound thread dials through")
        .containsExactly(seed);
    assertThat(peerList.get(identity.getKademliaId()))
        .as("the identity must resolve to the object that is in the list")
        .isSameAs(seed);
    assertThat(seed.getNodeId())
        .as("the placeholder adopts the identity, keys included")
        .isSameAs(identity);
  }

  /**
   * A different node answers at a known address (the peer behind it wiped its data, or the address
   * was reassigned). Two objects are correct here — they are two identities — but only one of them
   * may keep the address, otherwise the outbound thread has two dial candidates for one socket.
   */
  @Test
  void aDifferentIdentityAtAKnownAddressTakesTheAddressOver() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();

    NodeId oldIdentity = NodeId.generateWithSimpleKey();
    Peer before = new Peer("10.0.0.3", 59558, oldIdentity);
    peerList.add(before);

    NodeId newIdentity = NodeId.generateWithSimpleKey();
    Peer after = new Peer("10.0.0.3", 59558, newIdentity);
    peerList.add(after);

    assertThat(peerList.snapshot()).containsExactlyInAnyOrder(before, after);
    assertThat(before.isDialable())
        .as("the old identity must not stay a dial candidate for an address it no longer owns")
        .isFalse();
    assertThat(after.isDialable()).isTrue();
    assertThat(peerList.get(oldIdentity.getKademliaId())).isSameAs(before);
    assertThat(peerList.get(newIdentity.getKademliaId())).isSameAs(after);
  }

  /**
   * TD164: the peer's public key must survive learning an identity we already had. {@code
   * setNodeId(new NodeId(newId))} replaced a {@link NodeId} carrying the Ed25519/X25519 keys with a
   * key-less one — and those keys are what {@code PeerInHandshake.hasPublicKey()} and {@code
   * calculateSharedSecret()} read.
   */
  @Test
  void publicKeySurvivesUpdateKademliaIdWithTheSameIdentity() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();

    NodeId identity = NodeId.generateWithSimpleKey();
    Peer peer = new Peer("10.0.0.4", 59558, identity);
    peerList.add(peer);
    assertThat(peer.getNodeId().hasKey()).as("precondition").isTrue();

    peerList.updateKademliaId(peer, identity.getKademliaId());

    assertThat(peer.getNodeId().hasKey())
        .as("the peer's public key must not be dropped when the identity did not change")
        .isTrue();
    assertThat(peer.getNodeId()).isSameAs(identity);
    assertThat(peerList.get(identity.getKademliaId())).isSameAs(peer);
  }

  /**
   * A genuinely new identity does invalidate the keys we had: they belong to the previous keypair,
   * so a key-less NodeId (which makes the handshake request the public key again) is correct.
   */
  @Test
  void aGenuineIdentityChangeDropsTheKeysOfThePreviousIdentity() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();

    NodeId oldIdentity = NodeId.generateWithSimpleKey();
    Peer peer = new Peer("10.0.0.5", 59558, oldIdentity);
    peerList.add(peer);

    KademliaId newId = new KademliaId();
    peerList.updateKademliaId(peer, newId);

    assertThat(peer.getKademliaId()).isEqualTo(newId);
    assertThat(peer.getNodeId().hasKey())
        .as("keys of the old identity would be wrong for the new one")
        .isFalse();
    assertThat(peerList.get(newId)).isSameAs(peer);
    assertThat(peerList.get(oldIdentity.getKademliaId())).isNull();
  }

  /**
   * TD162, second half: {@code updateKademliaId} used to {@code put} the peer under an identity
   * another object already owned, dropping that object out of the identity index while leaving it
   * in the array list — manufacturing the duplicate pair itself.
   */
  @Test
  void updateKademliaIdHandsBackTheOwnerInsteadOfCreatingASecondObject() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();

    NodeId identity = NodeId.generateWithSimpleKey();
    Peer owner = new Peer("10.0.0.6", 59558, identity);
    peerList.add(owner);
    peerList.clearConnectionDetails(owner); // the address was cleared earlier, as production does

    // the seed entry we dialled, which turns out to be the very same node
    Peer dialled = new Peer("10.0.0.7", 59558);
    peerList.add(dialled);

    Peer result = peerList.updateKademliaId(dialled, identity.getKademliaId());

    assertThat(result).as("the caller must continue on the owner").isSameAs(owner);
    assertThat(peerList.snapshot()).containsExactly(owner);
    assertThat(peerList.get(identity.getKademliaId())).isSameAs(owner);
    assertThat(owner.getIp())
        .as("the address of the dropped duplicate is the only one we have for this node")
        .isEqualTo("10.0.0.7");
    assertThat(peerList.add(new Peer("10.0.0.7", 59558)))
        .as("the address index must point at the owner")
        .isSameAs(owner);
  }

  /**
   * TD165: a peer whose selection key was cancelled while it sat in the read queue used to be
   * logged (with the misspelling "key was canneled") and left {@code connected}, with no OP_READ
   * interest restored — nobody reads it any more, but every duplicate guard counts it as a live
   * connection.
   */
  @Test
  void aCancelledSelectionKeyDisconnectsThePeerInsteadOfLeavingItUnread() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);

    try (Selector selector = Selector.open();
        SocketChannel channel = SocketChannel.open()) {
      channel.configureBlocking(false);
      SelectionKey key = channel.register(selector, SelectionKey.OP_READ);

      Peer peer = new Peer("10.0.0.8", 59558, NodeId.generateWithSimpleKey());
      peer.setConnected(true);
      peer.setSocketChannel(channel);
      peer.setSelectionKey(key);

      key.cancel();

      connectionHandler.finishedReadingPeer(peer);

      assertThat(peer.isConnected())
          .as("a peer behind a cancelled key is unreachable and must not keep its slot")
          .isFalse();
    }
  }

  /**
   * The counterpart: a peer that has meanwhile adopted a <em>new</em> connection must survive the
   * cancelled key of the old one. {@code finishedReadingPeer} only tears the peer down while the
   * cancelled key is still the peer's current one.
   */
  @Test
  void aLiveConnectionIsKeptAndPutBackUnderOpRead() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);

    try (Selector selector = Selector.open();
        SocketChannel channel = SocketChannel.open()) {
      channel.configureBlocking(false);
      SelectionKey key = channel.register(selector, 0);

      Peer peer = new Peer("10.0.0.9", 59558, NodeId.generateWithSimpleKey());
      peer.setConnected(true);
      peer.setSocketChannel(channel);
      peer.setSelectionKey(key);

      connectionHandler.finishedReadingPeer(peer);

      assertThat(peer.isConnected()).isTrue();
      assertThat(key.interestOps() & SelectionKey.OP_READ)
          .as("the peer has to go back under OP_READ, that is the whole point of this method")
          .isNotZero();
    }
  }

  /** A peer without any selection key cannot be read either. */
  @Test
  void aConnectedPeerWithoutASelectionKeyIsDisconnected() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);

    Peer peer = new Peer("10.0.0.10", 59558, NodeId.generateWithSimpleKey());
    peer.setConnected(true);

    connectionHandler.finishedReadingPeer(peer);

    assertThat(peer.isConnected()).isFalse();
  }

  /** TD143: the dial claim refuses a peer that already has a connection. */
  @Test
  void claimForDialRefusesAConnectedPeer() {
    Peer peer = new Peer("10.0.0.11", 59558, NodeId.generateWithSimpleKey());
    assertThat(OutboundHandler.claimForDial(peer)).isTrue();
    assertThat(peer.isConnecting).isTrue();

    Peer connected = new Peer("10.0.0.12", 59558, NodeId.generateWithSimpleKey());
    connected.setConnected(true);
    assertThat(OutboundHandler.claimForDial(connected)).isFalse();
  }

  /**
   * TD143, the window that matters: {@code setupConnectionForPeer} clears {@code connected} and
   * {@code isConnecting} before it raises them again, and the outbound thread reads those flags
   * from an unsynchronised snapshot. A dial that starts inside that window opens a redundant
   * parallel connection, which the far side swaps in and which therefore tears the just-established
   * connection down. The claim takes the peer's {@code writeBufferLock}, the lock the swap holds,
   * so it can only ever see the state before or after — never during.
   */
  @Test
  void claimForDialCannotObserveAPeerInTheMiddleOfAConnectionSwap() throws Exception {
    Peer peer = new Peer("10.0.0.13", 59558, NodeId.generateWithSimpleKey());
    peer.setConnected(true);

    AtomicBoolean claimed = new AtomicBoolean(true);
    CountDownLatch dialStarted = new CountDownLatch(1);
    CountDownLatch dialFinished = new CountDownLatch(1);

    peer.getWriteBufferLock().lock();
    Thread dialer =
        new Thread(
            () -> {
              dialStarted.countDown();
              claimed.set(OutboundHandler.claimForDial(peer));
              dialFinished.countDown();
            });
    dialer.setDaemon(true);
    try {
      dialer.start();
      assertThat(dialStarted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(dialFinished.await(300, TimeUnit.MILLISECONDS))
          .as("the claim must wait for the swap instead of reading the flags mid-transition")
          .isFalse();

      // the swap: disconnect() clears both flags, setConnected(true) raises connected again
      peer.disconnect("new connection for this peer");
      peer.setConnected(true);
    } finally {
      peer.getWriteBufferLock().unlock();
    }

    assertThat(dialFinished.await(5, TimeUnit.SECONDS)).isTrue();
    dialer.join(5000);
    assertThat(claimed.get())
        .as("no redundant parallel connection to a peer that just got one")
        .isFalse();
  }

  /**
   * ... and the address is not taken from a peer we are connected to: {@code
   * PeerExchangeHandler.handleSendPeerList} builds peers from an unauthenticated ip/port/identity
   * triple, so anyone could otherwise make us forget how to reach a live peer.
   */
  @Test
  void aClaimOnTheAddressOfALivePeerIsNotHonoured() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();

    NodeId liveIdentity = NodeId.generateWithSimpleKey();
    Peer live = new Peer("10.0.0.14", 59558, liveIdentity);
    peerList.add(live);
    live.setConnected(true);

    NodeId gossiped = NodeId.generateWithSimpleKey();
    peerList.add(new Peer("10.0.0.14", 59558, gossiped));

    assertThat(live.isDialable()).as("the address of a live peer must survive a claim").isTrue();
    assertThat(peerList.get(gossiped.getKademliaId()).isDialable())
        .as("the claiming peer is registered, but without the address it claimed")
        .isFalse();
    assertThat(peerList.add(new Peer("10.0.0.14", 59558)))
        .as("the address index still points at the live peer")
        .isSameAs(live);
  }
}
