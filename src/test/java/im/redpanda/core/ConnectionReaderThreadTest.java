package im.redpanda.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.junit.Test;

public class ConnectionReaderThreadTest {

  /**
   * sdd02 phase 1: a v22 light-client handshake is rejected (channel closed) and counted in {@link
   * ConnectionReaderThread#REJECTED_LEGACY_V22_ATTEMPTS} — in-process twin of the E2E reject test
   * so the branch shows up in unit-test coverage.
   */
  @Test
  public void v22LightClientHandshakeIsRejectedAndCounted() throws Exception {
    try (SocketChannel channel = SocketChannel.open()) {
      PeerInHandshake peerInHandshake = new PeerInHandshake("127.0.0.1", channel);
      long before = ConnectionReaderThread.REJECTED_LEGACY_V22_ATTEMPTS.get();

      boolean accepted =
          ConnectionReaderThread.parseHandshake(
              new ServerContext(), peerInHandshake, handshake(22, (byte) 160));

      assertFalse("v22 light client must be rejected after the shutdown", accepted);
      assertFalse("channel must be closed on reject", channel.isOpen());
      assertEquals(before + 1, ConnectionReaderThread.REJECTED_LEGACY_V22_ATTEMPTS.get());
    }
  }

  /** Unknown protocol versions are rejected too, but do not count as legacy v22 attempts. */
  @Test
  public void unknownVersionRejectDoesNotCountAsLegacyAttempt() throws Exception {
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

  private static ByteBuffer handshake(int version, byte clientType) {
    ByteBuffer handshake = ByteBuffer.allocate(30);
    handshake.put(Server.MAGIC.getBytes());
    handshake.put((byte) version);
    handshake.put(clientType); // > 128 as unsigned byte marks a light client
    handshake.put(new byte[20]); // KademliaId
    handshake.putInt(0); // port
    handshake.flip();
    return handshake;
  }
}
