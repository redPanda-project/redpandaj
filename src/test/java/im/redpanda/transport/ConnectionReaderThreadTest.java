package im.redpanda.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.junit.jupiter.api.Test;

class ConnectionReaderThreadTest {

  /**
   * sdd02 phase 1: a v22 light-client handshake is rejected (channel closed) and counted in {@link
   * ConnectionReaderThread#REJECTED_LEGACY_V22_ATTEMPTS} — in-process twin of the E2E reject test
   * so the branch shows up in unit-test coverage.
   */
  @Test
  void v22LightClientHandshakeIsRejectedAndCounted() throws Exception {
    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", channel);
      long before = ConnectionReaderThread.REJECTED_LEGACY_V22_ATTEMPTS.get();

      boolean accepted =
          ConnectionReaderThread.parseHandshake(
              new ServerContext(), peerInHandshake, handshake(22, (byte) 160));

      assertFalse(accepted, "v22 light client must be rejected after the shutdown");
      assertFalse(channel.isOpen(), "channel must be closed on reject");
      assertEquals(before + 1, ConnectionReaderThread.REJECTED_LEGACY_V22_ATTEMPTS.get());
    }
  }

  /** Unknown protocol versions are rejected too, but do not count as legacy v22 attempts. */
  @Test
  void unknownVersionRejectDoesNotCountAsLegacyAttempt() throws Exception {
    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", channel);
      long before = ConnectionReaderThread.REJECTED_LEGACY_V22_ATTEMPTS.get();

      boolean accepted =
          ConnectionReaderThread.parseHandshake(
              new ServerContext(), peerInHandshake, handshake(21, (byte) 160));

      assertFalse(accepted);
      assertFalse(channel.isOpen());
      assertEquals(before, ConnectionReaderThread.REJECTED_LEGACY_V22_ATTEMPTS.get());
    }
  }

  /** Fewer than 30 bytes is "not complete yet", not a reject: the channel stays open. */
  @Test
  void partialHandshakeIsNotRejected() throws Exception {
    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", channel);

      boolean accepted =
          ConnectionReaderThread.parseHandshake(
              new ServerContext(), peerInHandshake, ByteBuffer.allocate(29).limit(29));

      assertFalse(accepted);
      assertTrue(channel.isOpen(), "a partial handshake must not close the channel");
    }
  }

  /** The advertised port is read as a signed int, so it has to be range-checked. */
  @Test
  void handshakeWithAnOutOfRangePortIsRejected() throws Exception {
    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", channel);

      boolean accepted =
          ConnectionReaderThread.parseHandshake(
              new ServerContext(), peerInHandshake, handshake(Server.VERSION, (byte) 0, 70000));

      assertFalse(accepted);
    }
  }

  /** Our own KademliaId on the wire means we dialled ourselves; the connection is dropped. */
  @Test
  void handshakeFromOurselvesIsRejectedAndClosed() throws Exception {
    ServerContext ctx = ServerContext.buildDefaultServerContext();

    ByteBuffer handshake = ByteBuffer.allocate(30);
    handshake.put(Server.MAGIC.getBytes());
    handshake.put((byte) Server.VERSION);
    handshake.put((byte) 0);
    handshake.put(ctx.getOwnNodeId().getBytes());
    handshake.putInt(1234);
    handshake.flip();

    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", channel);

      assertFalse(ConnectionReaderThread.parseHandshake(ctx, peerInHandshake, handshake));
      assertEquals(2, peerInHandshake.getStatus(), "status 2 is the disconnect code");
      assertFalse(channel.isOpen());
    }
  }

  private static ByteBuffer handshake(int version, byte clientType) {
    return handshake(version, clientType, 0);
  }

  private static ByteBuffer handshake(int version, byte clientType, int port) {
    ByteBuffer handshake = ByteBuffer.allocate(30);
    handshake.put(Server.MAGIC.getBytes());
    handshake.put((byte) version);
    handshake.put(clientType); // > 128 as unsigned byte marks a light client
    handshake.put(new byte[20]); // KademliaId
    handshake.putInt(port);
    handshake.flip();
    return handshake;
  }
}
