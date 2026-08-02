package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for the two {@code oldPeer != null} outcomes of {@code peerList.add(peerOrigin)}
 * inside {@link ConnectionHandler#setupConnection}:
 *
 * <ul>
 *   <li><b>TD020 (parallel-handshake orphan):</b> when {@code oldPeer != peerOrigin}, two inbound
 *       connections from the same identity raced — both saw {@code peerList.get(identity) == null}
 *       during {@code parseHandshake} and built separate, fully connected {@code Peer} objects, and
 *       only the first got registered. The loser ({@code peerOrigin}) must be <em>disconnected</em>
 *       so no unregistered, still-reading peer object survives; the pre-registered winner stays.
 *   <li><b>TD019 (reconnect diagnostic):</b> when {@code oldPeer == peerOrigin} — the sequential
 *       half-open reconnect (T54), where {@code parseHandshake} found the already-registered peer
 *       and the channel/stream swap already happened in {@link Peer#setupConnectionForPeer} (PR
 *       #271) — {@code setupConnection} only logs a diagnostic and leaves that peer registered and
 *       connected.
 * </ul>
 */
class ConnectionHandlerDuplicateConnectionTest {

  static {
    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /**
   * TD020: two inbound connections from the same node complete their handshakes in parallel. Both
   * built distinct, connected {@link Peer} objects because neither saw the other in the PeerList
   * yet; the first (winner) is already registered. Driving the second (loser) through {@code
   * setupConnection} must disconnect it — it is otherwise a silent orphan: connected and read by
   * the selector, but unreachable for outbound because {@code peerList.get(identity)} returns the
   * winner.
   */
  @Test
  void parallelHandshakeLoserIsDisconnectedAndNotRegistered() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);

    NodeId identity = NodeId.generateWithSimpleKey();

    // The winner: a distinct, fully connected peer for this identity is already registered.
    Peer winner = new Peer("127.0.0.1", 0, identity);
    winner.setConnected(true);
    serverContext.getPeerList().add(winner);

    // The loser: a second physical connection from the same identity finishes its handshake.
    Peer loser = new Peer("127.0.0.1", 0, identity);
    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", channel);
      peerInHandshake.setPeer(loser);
      peerInHandshake.setLightClient(true); // skip the Node/DB lookups in setupConnection
      peerInHandshake.setIdentity(identity.getKademliaId());
      peerInHandshake.setNodeId(identity);
      peerInHandshake.setKey(new NoopSelectionKey());
      connectionHandler.addPeerInHandshake(peerInHandshake);

      connectionHandler.setupConnection(loser, peerInHandshake);

      // The winner stays the registered peer; the loser is NOT registered in its place.
      Peer registered = serverContext.getPeerList().get(identity.getKademliaId());
      assertThat(registered)
          .as("the pre-registered winner must remain the registered peer for this identity")
          .isSameAs(winner);
      assertThat(registered)
          .as("setupConnection must not register the racing duplicate")
          .isNotSameAs(loser);

      // The core of TD020: the loser must be torn down, not left as a connected, still-reading
      // orphan.
      assertThat(loser.isConnected())
          .as("the losing parallel duplicate must be disconnected, not orphaned")
          .isFalse();
      assertThat(channel.isOpen()).as("the losing duplicate's socket must be closed").isFalse();
      assertThat(winner.isConnected())
          .as("disconnecting the loser must not touch the winner's connection")
          .isTrue();
    }
  }

  /**
   * TD019 diagnostic branch: a sequential reconnect where {@code parseHandshake} found the
   * already-registered peer, so {@code peerOrigin} <em>is</em> that same registered object. {@code
   * peerList.add()} returns it ({@code oldPeer == peerOrigin}); {@code setupConnection} must leave
   * it registered and connected (the channel swap already happened in {@code
   * setupConnectionForPeer}) and must NOT take the TD020 disconnect path.
   */
  @Test
  void reconnectOfSameRegisteredPeerHitsDiagnosticBranchAndStaysConnected() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);

    NodeId identity = NodeId.generateWithSimpleKey();

    // This very peer is already registered; a fresh connection for it now completes (reconnect).
    Peer peer = new Peer("127.0.0.1", 0, identity);
    serverContext.getPeerList().add(peer);

    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", channel);
      peerInHandshake.setPeer(peer); // parseHandshake found and reused the registered peer
      peerInHandshake.setLightClient(true); // skip the Node/DB lookups in setupConnection
      peerInHandshake.setIdentity(identity.getKademliaId());
      peerInHandshake.setNodeId(identity);
      peerInHandshake.setKey(new NoopSelectionKey());
      connectionHandler.addPeerInHandshake(peerInHandshake);

      connectionHandler.setupConnection(peer, peerInHandshake);

      Peer registered = serverContext.getPeerList().get(identity.getKademliaId());
      assertThat(registered)
          .as("the reconnecting peer stays the registered peer for this identity")
          .isSameAs(peer);
      assertThat(peer.isConnected())
          .as("a reconnect must leave the peer connected, not take the TD020 disconnect path")
          .isTrue();
    }
  }

  /**
   * T88: a light client that was evicted from the peer list while it was away reconnects. Nothing
   * is registered for its identity any more, so {@code parseHandshake} builds a fresh {@link Peer}
   * — {@code setupConnection} must register it, not mistake it for the losing half of a parallel
   * handshake.
   *
   * <p>It did, before the fix: {@code PeerList.remove} skipped the ip+port map for {@code port ==
   * 0}, so the evicted peer stayed reachable through {@code add()}'s ip+port branch and came back
   * as {@code oldPeer != peerOrigin} — the TD020 signature. Every reconnect was dropped, on every
   * retry, which is what wedged the S4 airplane-mode scenario (154 duplicate lines, no delivery).
   */
  @Test
  void reconnectAfterEvictionIsRegisteredInsteadOfDroppedAsDuplicate() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);
    PeerList peerList = serverContext.getPeerList();

    NodeId identity = NodeId.generateWithSimpleKey();

    // The connection before the outage, and its eviction while the client was unreachable (T86:
    // PeerJobs drops peers that are neither connected nor dialable, and a light client is both).
    Peer beforeTheOutage = new Peer("127.0.0.1", 0, identity);
    peerList.add(beforeTheOutage);
    peerList.remove(beforeTheOutage);
    assertThat(peerList.get(identity.getKademliaId()))
        .as("precondition: the identity is gone from the peer list")
        .isNull();

    // The reconnect: a fresh Peer, exactly as ConnectionReaderThread.parseHandshake builds it when
    // peerList.get(identity) returns null.
    Peer reconnect = new Peer("127.0.0.1", 0, new NodeId(identity.getKademliaId()));
    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", channel);
      peerInHandshake.setPeer(reconnect);
      peerInHandshake.setLightClient(true); // skip the Node/DB lookups in setupConnection
      peerInHandshake.setIdentity(identity.getKademliaId());
      peerInHandshake.setNodeId(identity);
      peerInHandshake.setKey(new NoopSelectionKey());
      connectionHandler.addPeerInHandshake(peerInHandshake);

      connectionHandler.setupConnection(reconnect, peerInHandshake);

      assertThat(peerList.get(identity.getKademliaId()))
          .as("the reconnecting light client must be registered")
          .isSameAs(reconnect);
      assertThat(reconnect.isConnected())
          .as("the reconnect must not be torn down as a TD020 duplicate")
          .isTrue();
      assertThat(channel.isOpen()).as("the reconnect's socket must stay open").isTrue();
    }
  }

  /**
   * Minimal {@link SelectionKey} stub: {@code setupConnection} calls the final {@code attach}, and
   * (on the TD020 disconnect path) {@code Peer.disconnect} calls {@code cancel}.
   */
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
