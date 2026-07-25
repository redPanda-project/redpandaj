package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.junit.Test;

/**
 * TD019 regression: {@link ConnectionHandler#setupConnection} contains a diagnostic-only branch
 * that fires when {@code peerList.add(peerOrigin)} returns an already-connected {@code oldPeer}
 * (i.e. a peer with the same identity is already registered and connected). The T54 analysis
 * established that {@code PeerList.add()} only ever returns a non-null {@code oldPeer} whose {@code
 * NodeId} equals {@code peerOrigin}'s, which is why the former "same node with same id" sub-branch
 * was removed as dead code. This test drives a light-client {@code setupConnection} against a pre-
 * registered connected duplicate so that diagnostic branch is actually exercised and cannot
 * silently rot; it also pins the observable outcome: the pre-existing (old) peer stays the
 * registered peer for that identity — {@code setupConnection} does not double-register the new one.
 */
public class ConnectionHandlerDuplicateConnectionTest {

  static {
    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void setupConnectionWithAlreadyConnectedDuplicateHitsDiagnosticBranchAndKeepsOldPeer()
      throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);

    NodeId identity = NodeId.generateWithSimpleKey();

    // A peer with this identity is already registered and connected.
    Peer alreadyConnected = new Peer("127.0.0.1", 0, identity);
    alreadyConnected.setConnected(true);
    serverContext.getPeerList().add(alreadyConnected);

    // A second physical connection from the same identity now completes its handshake.
    Peer incoming = new Peer("127.0.0.1", 0, identity);
    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", channel);
      peerInHandshake.setPeer(incoming);
      peerInHandshake.setLightClient(true); // skip the Node/DB lookups in setupConnection
      peerInHandshake.setIdentity(identity.getKademliaId());
      peerInHandshake.setNodeId(identity);
      peerInHandshake.setKey(new NoopSelectionKey());
      connectionHandler.addPeerInHandshake(peerInHandshake);

      connectionHandler.setupConnection(incoming, peerInHandshake);

      // peerList.add() returned the already-connected peer (the oldPeer diagnostic branch, TD019);
      // the incoming duplicate was NOT registered in its place.
      Peer registered = serverContext.getPeerList().get(identity.getKademliaId());
      assertThat(registered)
          .as("the already-connected peer must remain the registered peer for this identity")
          .isSameAs(alreadyConnected);
      assertThat(registered)
          .as("setupConnection must not double-register the incoming duplicate")
          .isNotSameAs(incoming);
    }
  }

  /**
   * Minimal {@link SelectionKey} stub: {@code setupConnection} only calls the final {@code attach}.
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
