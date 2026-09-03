package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * TD142 — the node1&lt;-&gt;node2 reconnect loop observed on the testnet on 2026-09-03.
 *
 * <p>The peer list can hold two {@link Peer} objects for the same node: an id-less seed/restored
 * entry and a handshake-built one share an address ({@code PeerList.addLocked}'s ip+port branch),
 * or {@code PeerList.updateKademliaId} moves the identity onto the second object. Only one of them
 * is reachable through {@code peerList.get(identity)}; both are in {@code snapshot()} and are
 * therefore dial candidates for {@link OutboundHandler}.
 *
 * <p>Dialling through the unregistered one used to be fatal, because the two ends of that socket
 * applied opposite duplicate-connection policies: the accepting side swapped the fresh socket in
 * ({@link Peer#setupConnectionForPeer}, "newest wins", T54) and thereby tore down its own working
 * connection, and a few milliseconds later the dialling side closed that very socket as a
 * "duplicate parallel connection" ("oldest wins"). Both ends ended up with no connection, redialled
 * 1-4 s later (the {@code OutboundHandler} pass interval) and repeated it — ~25 redials/min for as
 * long as the two objects existed, with {@code ss} showing no TCP connection between the nodes.
 */
class ConnectionHandlerReconnectLoopTest {

  static {
    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /**
   * The pinning test: a connection that completes on an unregistered duplicate peer object must be
   * kept (the far side has already committed to it), not closed, and the duplicate object must
   * leave the peer list so the outbound thread cannot dial through it again.
   */
  @Test
  void connectionCompletedOnUnregisteredDuplicateIsKeptAndTheDuplicateIsDropped() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);
    PeerList peerList = serverContext.getPeerList();

    NodeId identity = NodeId.generateWithSimpleKey();

    // The registered peer for that node, as it exists after an inbound handshake.
    Peer registered = new Peer("10.0.0.2", 59558, identity);
    peerList.add(registered);
    assertThat(peerList.get(identity.getKademliaId())).isSameAs(registered);

    // The second object for the very same node, sitting in the array list unreachable by identity
    // — the state the outbound thread dials through.
    Peer duplicate = new Peer("10.0.0.2", 59558, identity);
    forceIntoPeerList(peerList, duplicate);
    assertThat(peerList.snapshot()).contains(registered, duplicate);
    assertThat(peerList.get(identity.getKademliaId()))
        .as("precondition: the duplicate is NOT the registered peer")
        .isSameAs(registered);

    try (SocketChannel dialled = SocketChannel.open()) {
      PeerInHandshake peerInHandshake = new PeerInHandshake("10.0.0.2", duplicate, dialled);
      peerInHandshake.setLightClient(true); // skip the Node/DB lookups in setupConnection
      peerInHandshake.setIdentity(identity.getKademliaId());
      peerInHandshake.setNodeId(identity);
      peerInHandshake.setPort(59558);
      peerInHandshake.setKey(new NoopSelectionKey());
      connectionHandler.addPeerInHandshake(peerInHandshake);

      connectionHandler.setupConnection(duplicate, peerInHandshake);

      assertThat(dialled.isOpen())
          .as(
              "the far side has already adopted this socket; closing it is what drove the redial"
                  + " loop")
          .isTrue();
      assertThat(registered.getSocketChannel())
          .as("the registered peer must own the completed connection")
          .isSameAs(dialled);
      assertThat(registered.isConnected()).isTrue();
      assertThat(duplicate.isConnected())
          .as("the unregistered duplicate must never adopt a socket (TD020's actual requirement)")
          .isFalse();

      List<Peer> snapshot = peerList.snapshot();
      assertThat(snapshot)
          .as("the duplicate must be gone, otherwise the outbound thread dials it again")
          .doesNotContain(duplicate);
      assertThat(snapshot).contains(registered);
      assertThat(peerList.get(identity.getKademliaId()))
          .as("dropping the duplicate must not evict the registered peer")
          .isSameAs(registered);
    }
  }

  /**
   * TD020's orphan requirement, on the object that is dropped: if the duplicate already held a
   * connection of its own, that socket becomes unreachable the moment the object leaves the peer
   * list (nothing points at it any more, but the selector still reads it). It must be torn down —
   * the connection that just completed belongs to the handshake and goes to the registered peer.
   */
  @Test
  void aDuplicateThatHeldItsOwnConnectionIsTornDownWhenItIsDropped() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);
    PeerList peerList = serverContext.getPeerList();

    NodeId identity = NodeId.generateWithSimpleKey();
    Peer registered = new Peer("10.0.0.2", 59558, identity);
    peerList.add(registered);

    Peer duplicate = new Peer("10.0.0.2", 59558, identity);
    forceIntoPeerList(peerList, duplicate);

    try (SocketChannel duplicatesOwnSocket = SocketChannel.open();
        SocketChannel dialled = SocketChannel.open()) {
      duplicate.setConnected(true);
      duplicate.setSocketChannel(duplicatesOwnSocket);
      duplicate.setSelectionKey(new NoopSelectionKey());

      PeerInHandshake peerInHandshake = new PeerInHandshake("10.0.0.2", duplicate, dialled);
      peerInHandshake.setLightClient(true);
      peerInHandshake.setIdentity(identity.getKademliaId());
      peerInHandshake.setNodeId(identity);
      peerInHandshake.setPort(59558);
      peerInHandshake.setKey(new NoopSelectionKey());
      connectionHandler.addPeerInHandshake(peerInHandshake);

      connectionHandler.setupConnection(duplicate, peerInHandshake);

      assertThat(duplicatesOwnSocket.isOpen())
          .as("the dropped duplicate must not keep a socket the selector still reads")
          .isFalse();
      assertThat(duplicate.isConnected()).isFalse();
      assertThat(dialled.isOpen()).as("the completed connection is kept").isTrue();
      assertThat(registered.getSocketChannel()).isSameAs(dialled);
      assertThat(peerList.snapshot()).containsExactly(registered);
    }
  }

  /**
   * The other half of the same defect: {@code PeerList.remove(Peer)} resolves the peer by its
   * KademliaId, so removing a duplicate object evicted the <em>registered</em> peer and left the
   * duplicate as the only entry — turning a transient duplicate into a permanent one. {@link
   * PeerList#removeExact(Peer)} removes the object it was given.
   */
  @Test
  void removeExactDropsTheGivenObjectAndRemoveByIdWouldDropTheOtherOne() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    PeerList peerList = serverContext.getPeerList();
    NodeId identity = NodeId.generateWithSimpleKey();

    Peer registered = new Peer("10.0.0.2", 59558, identity);
    peerList.add(registered);
    Peer duplicate = new Peer("10.0.0.2", 59558, identity);
    forceIntoPeerList(peerList, duplicate);

    assertThat(peerList.removeExact(duplicate)).isTrue();
    assertThat(peerList.snapshot()).containsExactly(registered);
    assertThat(peerList.get(identity.getKademliaId()))
        .as("removeExact must not touch the identity mapping of the other object")
        .isSameAs(registered);

    // Contrast: the id-based removal evicts whoever owns the id, not the object handed in.
    Peer secondDuplicate = new Peer("10.0.0.2", 59558, identity);
    forceIntoPeerList(peerList, secondDuplicate);
    peerList.remove(secondDuplicate);
    assertThat(peerList.snapshot())
        .as("documents why removeExact exists: remove(Peer) drops the registered peer instead")
        .containsExactly(secondDuplicate);
  }

  /**
   * The peer list write lock is now taken <em>before</em> any connection state is moved, so {@code
   * peerOrigin} on the {@code PeerListBusyException} path can be the already-registered peer with a
   * live connection of its own. That connection must survive; only the handshake socket is dropped
   * (Copilot review, PR #339).
   */
  @Test
  void peerListLockContentionDropsOnlyTheHandshakeConnection() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);
    PeerList peerList = serverContext.getPeerList();

    NodeId identity = NodeId.generateWithSimpleKey();
    Peer registered = new Peer("10.0.0.2", 59558, identity);
    peerList.add(registered);

    try (SocketChannel liveSocket = SocketChannel.open();
        SocketChannel dialled = SocketChannel.open()) {
      registered.setConnected(true);
      registered.setSocketChannel(liveSocket);
      registered.setSelectionKey(new NoopSelectionKey());

      PeerInHandshake peerInHandshake = new PeerInHandshake("10.0.0.2", registered, dialled);
      peerInHandshake.setLightClient(true);
      peerInHandshake.setIdentity(identity.getKademliaId());
      peerInHandshake.setNodeId(identity);
      peerInHandshake.setPort(59558);
      peerInHandshake.setKey(new NoopSelectionKey());
      connectionHandler.addPeerInHandshake(peerInHandshake);

      // Hold the peer list write lock for longer than ConnectionHandler's budget.
      Thread hog =
          new Thread(
              () -> {
                peerList.getReadWriteLock().writeLock().lock();
                try {
                  Thread.sleep(ConnectionHandler.PEERLIST_LOCK_TIMEOUT_MS + 1500);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                } finally {
                  peerList.getReadWriteLock().writeLock().unlock();
                }
              });
      hog.setDaemon(true);
      hog.start();
      Thread.sleep(200);

      assertThat(connectionHandler.setupConnection(registered, peerInHandshake))
          .as("the connection could not be set up")
          .isFalse();

      assertThat(dialled.isOpen()).as("the handshake socket is dropped").isFalse();
      assertThat(registered.isConnected())
          .as("lock contention must not tear down an unrelated, live connection")
          .isTrue();
      assertThat(liveSocket.isOpen()).isTrue();

      hog.interrupt();
      hog.join(5000);
    }
  }

  /**
   * Puts a second {@link Peer} for an identity that is already registered into the array list, the
   * way {@code PeerList.addLocked}'s ip+port branch and {@code updateKademliaId} can. {@code add()}
   * itself short-circuits on the identity, so the state has to be built through the same list the
   * production paths end up mutating.
   */
  private static void forceIntoPeerList(PeerList peerList, Peer duplicate) throws Exception {
    java.lang.reflect.Field field = PeerList.class.getDeclaredField("peerArrayList");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<Peer> arrayList = (List<Peer>) field.get(peerList);
    arrayList.add(duplicate);
  }

  /** Minimal {@link SelectionKey} stub; {@code setupConnection} only attaches to it. */
  private static final class NoopSelectionKey extends SelectionKey {
    @Override
    public SelectableChannel channel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Selector selector() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValid() {
      return true;
    }

    @Override
    public void cancel() {
      // no-op
    }

    @Override
    public int interestOps() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SelectionKey interestOps(int ops) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int readyOps() {
      throw new UnsupportedOperationException();
    }
  }
}
