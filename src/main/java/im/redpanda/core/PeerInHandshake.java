package im.redpanda.core;

import im.redpanda.crypt.CryptoUtils;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.legacy.LegacyNodeId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.crypto.params.X25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.X25519PublicKeyParameters;

/**
 * Connection state during the TCP handshake. Supports two protocol versions:
 *
 * <ul>
 *   <li><b>v23 (MS03)</b>: 64-byte NodeId public key exchange (Ed25519 verify key + X25519
 *       encryption key), ephemeral X25519 key exchange, HKDF-SHA256 session keys, framed
 *       AES-256-GCM ({@link GcmFramedStreams}).
 *   <li><b>v22 (deprecated transition path for light clients)</b>: 65-byte brainpoolp256r1 key
 *       exchange, static ECDH + 8-byte randoms, AES-CTR streams ({@link LegacyCtrCipherStreams}).
 * </ul>
 */
public class PeerInHandshake {

  /** v22 legacy: AES-CTR IV length; each side contributes half of it as random bytes. */
  public static final int IVbytelen = 16;

  /** v22 legacy AES-CTR cipher. */
  @Deprecated(forRemoval = true)
  public static final String LEGACY_ALGORITHM = "AES/CTR/NoPadding";

  @Deprecated(forRemoval = true)
  public static final String LEGACY_PROVIDER = "SunJCE";

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

  // ---- v22 legacy state ----
  byte[] randomFromUs;
  byte[] randomFromThem;
  @Deprecated private LegacyNodeId legacyNodeIdFromThem;
  private SecretKey sharedSecretSend;
  private SecretKey sharedSecretReceive;
  private IvParameterSpec ivSend;
  private IvParameterSpec ivReceive;

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

  @Deprecated
  public LegacyNodeId getLegacyNodeIdFromThem() {
    return legacyNodeIdFromThem;
  }

  @Deprecated
  public void setLegacyNodeIdFromThem(LegacyNodeId legacyNodeIdFromThem) {
    this.legacyNodeIdFromThem = legacyNodeIdFromThem;
  }

  /**
   * Derives the session keys for this connection. v23: X25519(ephemeralA, ephemeralB) + HKDF-SHA256
   * with the sorted verify keys as salt. v22 (legacy): static brainpool ECDH + SHA256 over the
   * exchanged randoms.
   */
  public void calculateSharedSecret(ServerContext serverContext) {
    if (isProtocolV23()) {
      calculateSharedSecretV23(serverContext);
    } else {
      calculateSharedSecretV22(serverContext);
    }
  }

  private void calculateSharedSecretV23(ServerContext serverContext) {
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

  // -------------------------------------------------------------------------------------------
  // v22 legacy path (deprecated, light-client transition only)
  // -------------------------------------------------------------------------------------------

  @Deprecated
  public byte[] getRandomFromUs() {

    if (randomFromUs == null) {
      byte[] randomBytesForEncryption = new byte[PeerInHandshake.IVbytelen / 2];
      Server.secureRandom.nextBytes(randomBytesForEncryption);
      randomFromUs = randomBytesForEncryption;
    }

    return randomFromUs;
  }

  @Deprecated
  public byte[] getRandomFromThem() {
    return randomFromThem;
  }

  @Deprecated
  public void setRandomFromThem(byte[] randomFromThem) {
    this.randomFromThem = randomFromThem;
  }

  @Deprecated
  private void calculateSharedSecretV22(ServerContext serverContext) {

    if (legacyNodeIdFromThem == null) {
      throw new RuntimeException("calculateSharedSecret: legacy public key of peer unknown");
    }

    try {
      KeyAgreement keyAgreement = KeyAgreement.getInstance("ECDH", "BC");
      keyAgreement.init(serverContext.getLegacyNodeId().getKeyPair().getPrivate());
      keyAgreement.doPhase(legacyNodeIdFromThem.getKeyPair().getPublic(), true);

      SecretKey intermediateSharedSecret = keyAgreement.generateSecret("AES");

      byte[] encoded = intermediateSharedSecret.getEncoded();

      ByteBuffer bytesForPrivateAESkeySend = ByteBuffer.allocate(32 + PeerInHandshake.IVbytelen);
      ByteBuffer bytesForPrivateAESkeyReceive = ByteBuffer.allocate(32 + PeerInHandshake.IVbytelen);

      bytesForPrivateAESkeySend.put(encoded);
      bytesForPrivateAESkeyReceive.put(encoded);

      bytesForPrivateAESkeySend.put(randomFromUs);
      bytesForPrivateAESkeySend.put(randomFromThem);

      bytesForPrivateAESkeyReceive.put(randomFromThem);
      bytesForPrivateAESkeyReceive.put(randomFromUs);

      if (bytesForPrivateAESkeySend.remaining() != 0) {
        throw new RuntimeException("here is something wrong with the random bytes length!");
      }

      Sha256Hash sha256HashSend = Sha256Hash.create(bytesForPrivateAESkeySend.array());
      Sha256Hash sha256HashReceive = Sha256Hash.create(bytesForPrivateAESkeyReceive.array());

      sharedSecretSend = new SecretKeySpec(sha256HashSend.getBytes(), "AES");
      sharedSecretReceive = new SecretKeySpec(sha256HashReceive.getBytes(), "AES");

      ByteBuffer bytesForIVsend = ByteBuffer.allocate(IVbytelen);
      ByteBuffer bytesForIVreceive = ByteBuffer.allocate(IVbytelen);

      bytesForIVsend.put(randomFromUs);
      bytesForIVsend.put(randomFromThem);
      bytesForIVreceive.put(randomFromThem);
      bytesForIVreceive.put(randomFromUs);

      ivSend = new IvParameterSpec(bytesForIVsend.array());
      ivReceive = new IvParameterSpec(bytesForIVreceive.array());

    } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchProviderException e) {
      e.printStackTrace();
    }
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
    if (protocolVersion != 0 && !isProtocolV23()) {
      return legacyNodeIdFromThem != null;
    }
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
    if (isProtocolV23()) {
      if (sendKey == null || receiveKey == null) {
        throw new IllegalStateException("activateEncryption: session keys not derived yet");
      }
      peerChiperStreams = new GcmFramedStreams(sendKey, receiveKey);
      return;
    }

    // v22 legacy AES-CTR streams
    try {
      PeerOutputStream peerOutputStream = new PeerOutputStream();
      Cipher cipherSend = Cipher.getInstance(LEGACY_ALGORITHM, LEGACY_PROVIDER);
      cipherSend.init(Cipher.ENCRYPT_MODE, sharedSecretSend, ivSend);
      CipherOutputStreamByteBuffer cipherOutputStream =
          new CipherOutputStreamByteBuffer(peerOutputStream, cipherSend);

      PeerInputStream peerInputStream = new PeerInputStream();
      Cipher cipherReceive = Cipher.getInstance(LEGACY_ALGORITHM, LEGACY_PROVIDER);
      cipherReceive.init(Cipher.DECRYPT_MODE, sharedSecretReceive, ivReceive);
      CipherInputStreamByteBuffer cipherInputStream =
          new CipherInputStreamByteBuffer(peerInputStream, cipherReceive);

      peerChiperStreams =
          new LegacyCtrCipherStreams(
              peerOutputStream, peerInputStream, cipherInputStream, cipherOutputStream);

    } catch (NoSuchAlgorithmException
        | NoSuchPaddingException
        | InvalidKeyException
        | InvalidAlgorithmParameterException
        | NoSuchProviderException e) {
      e.printStackTrace();
    }
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
