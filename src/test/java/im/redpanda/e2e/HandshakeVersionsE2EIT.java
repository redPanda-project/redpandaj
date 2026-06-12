package im.redpanda.e2e;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import im.redpanda.core.Command;
import im.redpanda.core.GcmFramedStreams;
import im.redpanda.core.NodeId;
import im.redpanda.core.PeerInHandshake;
import im.redpanda.crypt.CryptoUtils;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.legacy.LegacyNodeId;
import im.redpanda.testutil.TestNodeProcess;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.crypto.params.X25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * MS03 acceptance: real TCP handshakes against a running node.
 *
 * <ul>
 *   <li>v23 light client: 64-byte key exchange, ephemeral X25519, HKDF, framed AES-256-GCM —
 *       ping/pong roundtrip works and a flipped bit in a frame kills the connection.
 *   <li>v22 legacy light client (pre-MS03 mobile app): 65-byte brainpool keys, AES-CTR — still
 *       accepted during the transition phase.
 * </ul>
 */
@SuppressWarnings("deprecation")
public class HandshakeVersionsE2EIT {

  private static final String MAGIC = "k3gV";
  private static final SecureRandom RANDOM = new SecureRandom();

  static {
    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void v23LightClientHandshakeWithFramedGcmAndTamperDetection() throws Exception {
    Path nodeDir = temporaryFolder.newFolder("nodeV23").toPath();
    int port = nextFreePort();

    try (TestNodeProcess node = TestNodeProcess.start(nodeDir, port, "", 0)) {
      assertTrue("node failed to start", node.awaitReady(Duration.ofSeconds(30)));

      // 1. happy path: handshake + encrypted ping/pong
      try (V23Client client = V23Client.connect(port)) {
        // the node sends its initial ping right after activating the encryption
        byte firstCommand = client.readEncryptedCommand();
        assertEquals(Command.PING, firstCommand);

        // like the mobile client our first encrypted command is an initial ping — the node
        // consumes it to complete the handshake (promotion to a full Peer)
        client.sendEncrypted(new byte[] {Command.PING});
        LightClientBase.pause();

        // a second ping is answered by the command processor with a pong — proves both
        // directions and frame counters work
        client.sendEncrypted(new byte[] {Command.PING});
        assertEquals(Command.PONG, client.readEncryptedCommand());
      }

      // 2. tampered frame: the node must drop the connection (no silent corruption)
      try (V23Client client = V23Client.connect(port)) {
        assertEquals(Command.PING, client.readEncryptedCommand());
        client.sendEncrypted(new byte[] {Command.PING});
        LightClientBase.pause();

        client.sendTamperedFrame(new byte[] {Command.PING});
        assertTrue("node must disconnect after a tampered frame", client.awaitDisconnect(10_000));
      }

      node.stop(Duration.ofSeconds(10));
    }
  }

  @Test
  public void v22LegacyLightClientIsStillAcceptedDuringTransition() throws Exception {
    Path nodeDir = temporaryFolder.newFolder("nodeV22").toPath();
    int port = nextFreePort();

    try (TestNodeProcess node = TestNodeProcess.start(nodeDir, port, "", 0)) {
      assertTrue("node failed to start", node.awaitReady(Duration.ofSeconds(30)));

      try (V22LegacyClient client = V22LegacyClient.connect(port)) {
        byte firstCommand = client.readEncryptedCommand();
        assertEquals(Command.PING, firstCommand);

        // initial ping completes the handshake, the second one is answered with a pong
        client.sendEncrypted(new byte[] {Command.PING});
        LightClientBase.pause();

        client.sendEncrypted(new byte[] {Command.PING});
        assertEquals(Command.PONG, client.readEncryptedCommand());
      }

      node.stop(Duration.ofSeconds(10));
    }
  }

  private int nextFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  // ----------------------------------------------------------------------------------------
  // minimal protocol clients (mirroring what the mobile light client does)
  // ----------------------------------------------------------------------------------------

  /** Shared plumbing: blocking reads with timeout, plaintext handshake helpers. */
  private abstract static class LightClientBase implements AutoCloseable {
    final Socket socket;
    final InputStream in;
    final OutputStream out;

    LightClientBase(int port) throws IOException {
      socket = new Socket("127.0.0.1", port);
      socket.setSoTimeout(20_000);
      in = socket.getInputStream();
      out = socket.getOutputStream();
    }

    /**
     * The node's handshake handler processes one command per read event — like the mobile client we
     * briefly pause between handshake messages so they arrive in separate reads.
     */
    static void pause() {
      try {
        Thread.sleep(150);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    void sendHandshake(int version, byte[] kademliaId) throws IOException {
      ByteBuffer handshake = ByteBuffer.allocate(30);
      handshake.put(MAGIC.getBytes());
      handshake.put((byte) version);
      handshake.put((byte) 160); // light client marker
      handshake.put(kademliaId);
      handshake.putInt(0);
      out.write(handshake.array());
      out.flush();
      readFully(30); // the node's handshake
      pause();
    }

    byte[] readFully(int n) throws IOException {
      byte[] bytes = new byte[n];
      int off = 0;
      while (off < n) {
        int read = in.read(bytes, off, n - off);
        if (read == -1) {
          throw new EOFException("connection closed after " + off + "/" + n + " bytes");
        }
        off += read;
      }
      return bytes;
    }

    /** Answers REQUEST_PUBLIC_KEY commands until the expected command byte arrives. */
    byte readPlaintextCommandAnswering(byte[] ourPublicKeyExport, byte expected)
        throws IOException {
      while (true) {
        byte command = readFully(1)[0];
        if (command == Command.REQUEST_PUBLIC_KEY) {
          pause();
          ByteBuffer reply = ByteBuffer.allocate(1 + ourPublicKeyExport.length);
          reply.put(Command.SEND_PUBLIC_KEY);
          reply.put(ourPublicKeyExport);
          out.write(reply.array());
          out.flush();
          continue;
        }
        if (command == expected) {
          return command;
        }
        throw new IOException("unexpected command in handshake: " + command);
      }
    }

    boolean awaitDisconnect(long timeoutMillis) throws IOException {
      long deadline = System.currentTimeMillis() + timeoutMillis;
      socket.setSoTimeout(500);
      while (System.currentTimeMillis() < deadline) {
        try {
          if (in.read() == -1) {
            return true;
          }
        } catch (java.net.SocketTimeoutException retry) {
          // keep polling
        }
      }
      return false;
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }
  }

  /** Protocol v23: Ed25519/X25519 identity, ephemeral X25519, HKDF, framed AES-256-GCM. */
  private static final class V23Client extends LightClientBase {
    private GcmFramedStreams streams;
    private final ByteBuffer decrypted = ByteBuffer.allocate(64 * 1024);

    private V23Client(int port) throws IOException {
      super(port);
    }

    static V23Client connect(int port) throws IOException {
      V23Client client = new V23Client(port);
      client.handshake();
      return client;
    }

    private void handshake() throws IOException {
      NodeId identity = NodeId.generateWithSimpleKey();
      sendHandshake(23, identity.getKademliaId().getBytes());

      // ask for the node's public key; answer the node's REQUEST_PUBLIC_KEY on the way
      out.write(new byte[] {Command.REQUEST_PUBLIC_KEY});
      out.flush();
      readPlaintextCommandAnswering(identity.exportPublic(), Command.SEND_PUBLIC_KEY);
      byte[] nodePublicKey = readFully(NodeId.PUBLIC_KEYLEN);

      // exchange ephemeral X25519 keys
      pause();
      X25519PrivateKeyParameters ephemeral = new X25519PrivateKeyParameters(RANDOM);
      ByteBuffer activate = ByteBuffer.allocate(1 + 32);
      activate.put(Command.ACTIVATE_ENCRYPTION);
      activate.put(ephemeral.generatePublicKey().getEncoded());
      out.write(activate.array());
      out.flush();

      readPlaintextCommandAnswering(identity.exportPublic(), Command.ACTIVATE_ENCRYPTION);
      byte[] nodeEphemeral = readFully(32);

      // key schedule (we initiated the connection -> client role)
      byte[] shared =
          CryptoUtils.x25519(ephemeral, new X25519PublicKeyParameters(nodeEphemeral, 0));
      byte[] ourVerify = identity.getVerifyKeyBytes();
      byte[] nodeVerify = Arrays.copyOfRange(nodePublicKey, 0, 32);
      byte[] minKey = Arrays.compareUnsigned(ourVerify, nodeVerify) <= 0 ? ourVerify : nodeVerify;
      byte[] maxKey = minKey == ourVerify ? nodeVerify : ourVerify;
      byte[] clientKey =
          CryptoUtils.hkdfSha256(shared, minKey, PeerInHandshake.HKDF_INFO_TCP_CLIENT, 32);
      byte[] serverKey =
          CryptoUtils.hkdfSha256(shared, maxKey, PeerInHandshake.HKDF_INFO_TCP_SERVER, 32);
      streams = new GcmFramedStreams(clientKey, serverKey);
    }

    byte readEncryptedCommand() throws Exception {
      while (decrypted.position() == 0) {
        byte[] chunk = new byte[4096];
        int read = in.read(chunk);
        if (read == -1) {
          throw new EOFException("connection closed");
        }
        streams.decrypt(ByteBuffer.wrap(chunk, 0, read), decrypted);
      }
      decrypted.flip();
      byte command = decrypted.get();
      decrypted.compact();
      return command;
    }

    void sendEncrypted(byte[] plaintext) throws IOException {
      ByteBuffer frame = ByteBuffer.allocate(plaintext.length + GcmFramedStreams.FRAME_OVERHEAD);
      streams.encrypt(ByteBuffer.wrap(plaintext), frame);
      out.write(frame.array(), 0, frame.position());
      out.flush();
    }

    void sendTamperedFrame(byte[] plaintext) throws IOException {
      ByteBuffer frame = ByteBuffer.allocate(plaintext.length + GcmFramedStreams.FRAME_OVERHEAD);
      streams.encrypt(ByteBuffer.wrap(plaintext), frame);
      byte[] bytes = Arrays.copyOf(frame.array(), frame.position());
      bytes[bytes.length - 1] ^= 0x01; // flip one bit in the GCM tag
      out.write(bytes);
      out.flush();
    }
  }

  /** Protocol v22 (deprecated): brainpool ECDH + AES-CTR, exactly like the pre-MS03 mobile app. */
  private static final class V22LegacyClient extends LightClientBase {
    private Cipher encryptCipher;
    private Cipher decryptCipher;

    private V22LegacyClient(int port) throws IOException {
      super(port);
    }

    static V22LegacyClient connect(int port) throws Exception {
      V22LegacyClient client = new V22LegacyClient(port);
      client.handshake();
      return client;
    }

    private void handshake() throws Exception {
      LegacyNodeId identity = LegacyNodeId.generate();
      sendHandshake(22, identity.getKademliaId().getBytes());

      out.write(new byte[] {Command.REQUEST_PUBLIC_KEY});
      out.flush();
      readPlaintextCommandAnswering(identity.exportPublic(), Command.SEND_PUBLIC_KEY);
      byte[] nodePublicKey = readFully(LegacyNodeId.PUBLIC_KEYLEN);

      pause();
      byte[] randomFromUs = new byte[8];
      RANDOM.nextBytes(randomFromUs);
      ByteBuffer activate = ByteBuffer.allocate(1 + 8);
      activate.put(Command.ACTIVATE_ENCRYPTION);
      activate.put(randomFromUs);
      out.write(activate.array());
      out.flush();

      readPlaintextCommandAnswering(identity.exportPublic(), Command.ACTIVATE_ENCRYPTION);
      byte[] randomFromNode = readFully(8);

      // static-static ECDH exactly like the legacy client
      KeyAgreement keyAgreement = KeyAgreement.getInstance("ECDH", "BC");
      keyAgreement.init(identity.getKeyPair().getPrivate());
      keyAgreement.doPhase(LegacyNodeId.importPublic(nodePublicKey).getKeyPair().getPublic(), true);
      byte[] sharedSecret = keyAgreement.generateSecret("AES").getEncoded();

      SecretKey sendKey = legacyKey(sharedSecret, randomFromUs, randomFromNode);
      SecretKey receiveKey = legacyKey(sharedSecret, randomFromNode, randomFromUs);
      IvParameterSpec sendIv = legacyIv(randomFromUs, randomFromNode);
      IvParameterSpec receiveIv = legacyIv(randomFromNode, randomFromUs);

      encryptCipher = Cipher.getInstance("AES/CTR/NoPadding", "SunJCE");
      encryptCipher.init(Cipher.ENCRYPT_MODE, sendKey, sendIv);
      decryptCipher = Cipher.getInstance("AES/CTR/NoPadding", "SunJCE");
      decryptCipher.init(Cipher.DECRYPT_MODE, receiveKey, receiveIv);
    }

    private static SecretKey legacyKey(
        byte[] sharedSecret, byte[] firstRandom, byte[] secondRandom) {
      ByteBuffer keyBytes = ByteBuffer.allocate(sharedSecret.length + 16);
      keyBytes.put(sharedSecret);
      keyBytes.put(firstRandom);
      keyBytes.put(secondRandom);
      return new SecretKeySpec(Sha256Hash.create(keyBytes.array()).getBytes(), "AES");
    }

    private static IvParameterSpec legacyIv(byte[] firstRandom, byte[] secondRandom) {
      ByteBuffer iv = ByteBuffer.allocate(16);
      iv.put(firstRandom);
      iv.put(secondRandom);
      return new IvParameterSpec(iv.array());
    }

    byte readEncryptedCommand() throws Exception {
      byte[] encrypted = readFully(1);
      return decryptCipher.update(encrypted)[0];
    }

    void sendEncrypted(byte[] plaintext) throws Exception {
      out.write(encryptCipher.update(plaintext));
      out.flush();
    }
  }
}
