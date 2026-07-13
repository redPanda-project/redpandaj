package im.redpanda.core;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import im.redpanda.crypt.CryptoUtils;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.SecureRandom;
import java.util.Arrays;
import org.bouncycastle.crypto.params.X25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;
import org.junit.Test;

/**
 * REDPANDAJ-2DS unit-level regression: {@code ConnectionHandler.handlePeerInHandshake} must not
 * silently drop the peer's first GCM frame (counter 0) when it arrives coalesced with
 * ACTIVATE_ENCRYPTION in a single read(). This is a same-JVM twin of {@code
 * HandshakeVersionsE2EIT#v23HandshakeSurvivesCoalescedActivateEncryptionAndFirstFrame} that runs as
 * a plain unit test (part of the default {@code mvn verify}/coverage run) instead of spinning up a
 * separate node process under the {@code e2e} Maven profile.
 */
public class ConnectionHandlerCoalescedHandshakeTest {

  static {
    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void coalescedActivateEncryptionAndFirstFrameArePromotedInOneEvent() throws Exception {
    ByteBufferPool.init();
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    ConnectionHandler connectionHandler = new ConnectionHandler(serverContext, false);

    try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        Selector selector = Selector.open()) {
      serverSocketChannel.configureBlocking(false);
      serverSocketChannel.bind(new InetSocketAddress("127.0.0.1", 0));
      int port = serverSocketChannel.socket().getLocalPort();

      try (SocketChannel client = SocketChannel.open(new InetSocketAddress("127.0.0.1", port))) {
        SocketChannel accepted;
        do {
          accepted = serverSocketChannel.accept();
        } while (accepted == null);
        accepted.configureBlocking(false);

        // the peer's identity is already validated and we're just waiting for the encryption
        // switch - i.e. exactly the state handlePeerInHandshake is in right before it would parse
        // an incoming ACTIVATE_ENCRYPTION command.
        NodeId peerIdentity = NodeId.generateWithSimpleKey();
        PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", accepted);
        peerInHandshake.setPeer(new Peer("127.0.0.1", 0));
        peerInHandshake.setProtocolVersion(23);
        peerInHandshake.setLightClient(true); // skip the Node/DB lookups in setupConnection
        peerInHandshake.setIdentity(peerIdentity.getKademliaId());
        peerInHandshake.setNodeId(peerIdentity);
        peerInHandshake.getPeer().setNodeId(peerIdentity); // hasPublicKey() == true
        peerInHandshake.setStatus(-1);

        SelectionKey key = accepted.register(selector, SelectionKey.OP_READ);
        key.attach(peerInHandshake);
        peerInHandshake.setKey(key);
        connectionHandler.addPeerInHandshake(peerInHandshake);

        try {
          exerciseCoalescedHandshake(
              connectionHandler, serverContext, peerInHandshake, selector, client);
        } finally {
          connectionHandler.removePeerInHandshake(peerInHandshake);
        }
      }
    }
  }

  private void exerciseCoalescedHandshake(
      ConnectionHandler connectionHandler,
      ServerContext serverContext,
      PeerInHandshake peerInHandshake,
      Selector selector,
      SocketChannel client)
      throws Exception {
    NodeId peerIdentity = peerInHandshake.getNodeId();

    // fetch the server's ephemeral public key up front (lazily generated once, memoized) so we
    // (playing the remote peer) can derive the identical session keys independently - mirroring
    // what handlePeerInHandshake itself does when it later sends its own ACTIVATE_ENCRYPTION in
    // response to ours.
    byte[] serverEphemeralPublic = peerInHandshake.getEphemeralPublicFromUs();

    X25519PrivateKeyParameters clientEphemeral = new X25519PrivateKeyParameters(new SecureRandom());
    byte[] shared =
        CryptoUtils.x25519(
            clientEphemeral, new X25519PublicKeyParameters(serverEphemeralPublic, 0));

    byte[] serverVerify = serverContext.getNodeId().getVerifyKeyBytes();
    byte[] clientVerify = peerIdentity.getVerifyKeyBytes();
    byte[] minKey =
        Arrays.compareUnsigned(clientVerify, serverVerify) <= 0 ? clientVerify : serverVerify;
    byte[] maxKey = minKey == clientVerify ? serverVerify : clientVerify;
    byte[] clientKey =
        CryptoUtils.hkdfSha256(shared, minKey, PeerInHandshake.HKDF_INFO_TCP_CLIENT, 32);
    byte[] serverKey =
        CryptoUtils.hkdfSha256(shared, maxKey, PeerInHandshake.HKDF_INFO_TCP_SERVER, 32);
    // this PeerInHandshake was built via the "accepted incoming connection" constructor, so
    // initiatedByUs == false server-side: the server sends with serverKey and receives with
    // clientKey (see PeerInHandshake#calculateSharedSecretV23) - we mirror that from the other
    // end.
    GcmFramedStreams clientStreams = new GcmFramedStreams(clientKey, serverKey);

    // build ACTIVATE_ENCRYPTION (+ our ephemeral key) and our first encrypted frame (PING,
    // counter 0) back to back, and write them in a *single* write() call - exactly what lets the
    // kernel coalesce them into one read() server-side (REDPANDAJ-2DS repro).
    ByteBuffer activate = ByteBuffer.allocate(1 + 32);
    activate.put(Command.ACTIVATE_ENCRYPTION);
    activate.put(clientEphemeral.generatePublicKey().getEncoded());
    activate.flip();

    ByteBuffer firstFrame = ByteBuffer.allocate(1 + GcmFramedStreams.FRAME_OVERHEAD);
    clientStreams.encrypt(ByteBuffer.wrap(new byte[] {Command.PING}), firstFrame);
    firstFrame.flip();

    ByteBuffer combined = ByteBuffer.allocate(activate.remaining() + firstFrame.remaining());
    combined.put(activate).put(firstFrame);
    combined.flip();
    client.write(combined);

    // let the node process this single coalesced read()
    assertTrue("expected the accepted channel to become readable", selector.select(10_000) > 0);
    SelectionKey readyKey = selector.selectedKeys().iterator().next();

    Method handlePeerInHandshake =
        ConnectionHandler.class.getDeclaredMethod("handlePeerInHandshake", SelectionKey.class);
    handlePeerInHandshake.setAccessible(true);
    handlePeerInHandshake.invoke(connectionHandler, readyKey);

    // with the fix, the coalesced first frame (counter 0) was decrypted within this same event
    // and the peer got promoted out of the handshake list. Before the fix, the leftover bytes
    // were silently dropped and the peer would still be stuck here.
    assertFalse(
        "peer must have been promoted out of the handshake list in the same event that processed"
            + " the coalesced ACTIVATE_ENCRYPTION + first frame",
        ConnectionHandler.peerInHandshakes.contains(peerInHandshake));

    // the node always sends its own ping right after activating encryption, regardless of the
    // fix - reading it here just drains the socket so the accepted channel can be closed cleanly.
    client.configureBlocking(true);
    client.socket().setSoTimeout(5_000);
    ByteBuffer inbound = ByteBuffer.allocate(64);
    assertTrue("expected the node's own ping", client.read(inbound) > 0);
  }
}
