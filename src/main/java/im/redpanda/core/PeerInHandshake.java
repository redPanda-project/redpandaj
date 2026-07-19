package im.redpanda.core;

import im.redpanda.crypt.CryptoUtils;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.SecureRandom;
import java.util.Arrays;
import org.bouncycastle.crypto.params.X25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;

/**
 * Connection state during the TCP handshake (protocol v23, MS03): 64-byte NodeId public key
 * exchange (Ed25519 verify key + X25519 encryption key), ephemeral X25519 key exchange, HKDF-SHA256
 * session keys, framed AES-256-GCM ({@link GcmFramedStreams}). The v22 transition path
 * (brainpool/AES-CTR) was removed in sdd02 phase 2.
 */
public class PeerInHandshake {

  /** v23: HKDF info strings for the per-direction session keys. */
  public static final byte[] HKDF_INFO_TCP_CLIENT = "tcp-client".getBytes();

  public static final byte[] HKDF_INFO_TCP_SERVER = "tcp-server".getBytes();

  private static final SecureRandom EPHEMERAL_RANDOM = new SecureRandom();

  String ip;
  int port = 0;
  int status = 0;
  KademliaId identity;
  NodeId nodeId;
  Peer peer;
  SocketChannel socketChannel;
  SelectionKey key;
  boolean lightClient = false;
  int protocolVersion;

  /** True if we opened this connection (TCP client role for the v23 key schedule). */
  private final boolean initiatedByUs;

  boolean weSendOurRandom = false;
  boolean awaitingEncryption = false;
  boolean encryptionActive = false;

  // ---- v23 state ----
  private X25519PrivateKeyParameters ephemeralKeyFromUs;
  private byte[] ephemeralPublicFromThem;
  private byte[] sendKey;
  private byte[] receiveKey;

  private PeerChiperStreams peerChiperStreams;

  private final long createdAt;

  /** Incoming connection (accepted by our ServerSocketChannel). */
  public PeerInHandshake(String ip, SocketChannel socketChannel) {
    this.ip = ip;
    this.socketChannel = socketChannel;
    this.initiatedByUs = false;
    createdAt = System.currentTimeMillis();
  }

  /** Outgoing connection (initiated by us). */
  public PeerInHandshake(String ip, Peer peer, SocketChannel socketChannel) {
    this.ip = ip;
    this.peer = peer;
    this.socketChannel = socketChannel;
    this.initiatedByUs = true;
    createdAt = System.currentTimeMillis();
  }

  /**
   * 0 default value, before any handshake was parsed.
   *
   * <p>1 first handshake was parsed, here we are waiting to obtain more information of the peer
   * like the public key to finish the complete handshake. 2 do not connect, connected to ourselves
   * or blacklisted -1 handshake finished from our site, we do not expect more data before switching
   * to encryption. We are waiting for the switching byte to start the encryption.
   *
   * @param status
   */
  public void setStatus(int status) {
    this.status = status;
  }

  /**
   * For the status information see the setter method.
   *
   * @return
   */
  public int getStatus() {
    return status;
  }

  public void addConnection(boolean alreadyConnected) {
    try {
      socketChannel.configureBlocking(false);

      SelectionKey key = null;
      ConnectionHandler.selectorLock.lock();
      try {
        ConnectionHandler.selector.wakeup();

        if (alreadyConnected) {
          key = socketChannel.register(ConnectionHandler.selector, SelectionKey.OP_READ);
        } else {
          key = socketChannel.register(ConnectionHandler.selector, SelectionKey.OP_CONNECT);
        }
      } finally {
        ConnectionHandler.selectorLock.unlock();
      }

      key.attach(this);
      this.key = key;

      ConnectionHandler.selector.wakeup();
    } catch (IOException ex) {
      ex.printStackTrace();
      peer.disconnect("could not init connection....");
      return;
    }
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public KademliaId getIdentity() {
    return identity;
  }

  public void setIdentity(KademliaId nonce) {
    this.identity = nonce;
  }

  public Peer getPeer() {
    return peer;
  }

  public void setPeer(Peer peer) {
    this.peer = peer;
  }

  public void setKey(SelectionKey key) {
    this.key = key;
  }

  public SelectionKey getKey() {
    return key;
  }

  public SocketChannel getSocketChannel() {
    return socketChannel;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public boolean isLightClient() {
    return lightClient;
  }

  public void setNodeId(NodeId nodeId) {
    this.nodeId = nodeId;
  }

  public boolean isProtocolV23() {
    return protocolVersion >= 23;
  }

  /** True if we opened this connection — the TCP "client" role of the v23 key schedule. */
  public boolean isInitiatedByUs() {
    return initiatedByUs;
  }

  // -------------------------------------------------------------------------------------------
  // v23: ephemeral X25519 key exchange + HKDF key schedule
  // -------------------------------------------------------------------------------------------

  /** Our 32-byte ephemeral X25519 public key (the keypair is generated on first call). */
  public byte[] getEphemeralPublicFromUs() {
    if (ephemeralKeyFromUs == null) {
      ephemeralKeyFromUs = new X25519PrivateKeyParameters(EPHEMERAL_RANDOM);
    }
    return ephemeralKeyFromUs.generatePublicKey().getEncoded();
  }

  public void setEphemeralPublicFromThem(byte[] ephemeralPublicFromThem) {
    this.ephemeralPublicFromThem = ephemeralPublicFromThem;
  }

  public byte[] getEphemeralPublicFromThem() {
    return ephemeralPublicFromThem;
  }

  /**
   * Derives the session keys for this connection: X25519(ephemeralA, ephemeralB) + HKDF-SHA256 with
   * the sorted verify keys as salt (v23).
   */
  public void calculateSharedSecret(ServerContext serverContext) {
    if (nodeId == null || !nodeId.hasKey()) {
      throw new RuntimeException("calculateSharedSecret: missing the peers public NodeId keys");
    }
    if (ephemeralKeyFromUs == null || ephemeralPublicFromThem == null) {
      throw new RuntimeException("calculateSharedSecret: ephemeral keys not exchanged yet");
    }

    byte[] shared =
        CryptoUtils.x25519(
            ephemeralKeyFromUs, new X25519PublicKeyParameters(ephemeralPublicFromThem, 0));

    byte[] ourVerifyKey = serverContext.getNodeId().getVerifyKeyBytes();
    byte[] theirVerifyKey = nodeId.getVerifyKeyBytes();

    byte[] minKey =
        compareUnsigned(ourVerifyKey, theirVerifyKey) <= 0 ? ourVerifyKey : theirVerifyKey;
    byte[] maxKey = minKey == ourVerifyKey ? theirVerifyKey : ourVerifyKey;

    byte[] clientKey =
        CryptoUtils.hkdfSha256(shared, minKey, HKDF_INFO_TCP_CLIENT, CryptoUtils.AES_KEY_LEN);
    byte[] serverKey =
        CryptoUtils.hkdfSha256(shared, maxKey, HKDF_INFO_TCP_SERVER, CryptoUtils.AES_KEY_LEN);

    if (initiatedByUs) {
      sendKey = clientKey;
      receiveKey = serverKey;
    } else {
      sendKey = serverKey;
      receiveKey = clientKey;
    }
  }

  private static int compareUnsigned(byte[] a, byte[] b) {
    return Arrays.compareUnsigned(a, b);
  }

  public boolean isWeSendOurRandom() {
    return weSendOurRandom;
  }

  public void setWeSendOurRandom(boolean weSendOurRandom) {
    this.weSendOurRandom = weSendOurRandom;
  }

  public boolean isAwaitingEncryption() {
    return awaitingEncryption;
  }

  public void setAwaitingEncryption(boolean awaitingEncryption) {
    this.awaitingEncryption = awaitingEncryption;
  }

  /** True if we obtained the keys of the peer required to derive the session secret. */
  public boolean hasPublicKey() {
    if (getPeer().getNodeId() == null) {
      return false;
    }
    return getPeer().getNodeId().hasKey();
  }

  public boolean isEncryptionActive() {
    return encryptionActive;
  }

  public void activateEncryption() {
    encryptionActive = true;
    if (sendKey == null || receiveKey == null) {
      throw new IllegalStateException("activateEncryption: session keys not derived yet");
    }
    peerChiperStreams = new GcmFramedStreams(sendKey, receiveKey);
  }

  public PeerChiperStreams getPeerChiperStreams() {
    return peerChiperStreams;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public String getIp() {
    return ip;
  }

  public void setLightClient(boolean lightClient) {
    this.lightClient = lightClient;
  }

  public void setProtocolVersion(int protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  public int getProtocolVersion() {
    return protocolVersion;
  }
}
