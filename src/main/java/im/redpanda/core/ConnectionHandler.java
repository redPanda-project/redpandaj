/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;

import im.redpanda.core.exceptions.PeerProtocolException;
import im.redpanda.crypt.Utils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;

/**
 * @author robin
 */
public class ConnectionHandler extends Thread {

  private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger();

  public static Selector selector;
  public static final ReentrantLock selectorLock = new ReentrantLock();

  public static ArrayList<PeerInHandshake> peerInHandshakes = new ArrayList<>();
  @Getter private ReentrantLock peerInHandshakesLock = new ReentrantLock(false);
  public static BlockingQueue<Peer> peersToReadAndParse = new LinkedBlockingQueue<>(600);
  public static ArrayList<Peer> workingRead = new ArrayList<>();
  public static BlockingQueue<Peer> doneRead = new LinkedBlockingQueue<>(600);
  public static DecimalFormat df = new DecimalFormat("#.000");
  boolean startFurther;

  private final ServerContext serverContext;
  private final PeerList peerList;

  public ConnectionHandler(ServerContext serverContext, boolean startFurther) {
    this.startFurther = startFurther;
    this.serverContext = serverContext;
    this.peerList = serverContext.getPeerList();
  }

  static {
    try {
      selector = Selector.open();
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Returns the port which the ServerSocketChannel was bound to.
   *
   * @return
   */
  public int bind() {

    String forcedPort = System.getenv("PORT");

    int port = -1;
    ServerSocketChannel serverSocketChannel = null;
    try {
      serverSocketChannel = ServerSocketChannel.open();
      serverSocketChannel.configureBlocking(false);

      if (forcedPort != null) {
        port = Integer.parseInt(forcedPort);
        bindToSpecificPortWithBlocking(port, serverSocketChannel);
      } else {
        port = bindToNextAvailablePort(Settings.getStartPort(), serverSocketChannel);
      }

      addServerSocketChannel(serverSocketChannel);
    } catch (IOException ex) {
      ex.printStackTrace();
      if (serverSocketChannel != null) {
        try {
          serverSocketChannel.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return port;
  }

  private int bindToNextAvailablePort(int port, ServerSocketChannel serverSocketChannel) {
    logger.info("searching port to bind to...");
    boolean bound = false;
    while (!bound) {
      try {
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
        bound = true;
      } catch (Exception e) {
        System.out.println("could not bound to port: %s".formatted(port));
        port++;
      }
    }
    logger.info("bound successfully to port: %s".formatted(port));
    return port;
  }

  private void bindToSpecificPortWithBlocking(int port, ServerSocketChannel serverSocketChannel) {
    logger.info("bin to specific port %s ...".formatted(port));
    boolean bound = false;
    while (!bound) {
      try {
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
        bound = true;
      } catch (Exception e) {
        System.out.println("could not bound to port: %s, retry".formatted(port));
        try {
          sleep(1000L);
        } catch (InterruptedException ex) {
          currentThread().interrupt();
          ex.printStackTrace();
        }
      }
    }
    logger.info("bound successfully to port: %s".formatted(port));
  }

  void addServerSocketChannel(ServerSocketChannel serverSocketChannel) {
    try {
      selector.wakeup();
      serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
      selector.wakeup();
      Log.putStd("added ServerSocketChannel");

    } catch (IOException ex) {
      Logger.getLogger(ConnectionHandler.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void run() {

    final String orgName = currentThread().getName();
    if (!orgName.contains(" ")) {
      currentThread().setName("IncomingHandler");
    }

    setDefaultUncaughtExceptionHandler((thread, throwable) -> Log.putCritical(throwable));

    ConnectionReaderThread.init(serverContext);

    while (!Server.isShuttingDown()) {

      readPeersBackToSelector();

      int readyChannels = 0;
      try {
        selectorLock.lock();
        selectorLock.unlock();
        readyChannels = selector.select();
      } catch (Exception e) {
        e.printStackTrace();
        try {
          sleep(100);
          System.out.println("exception in selector");
        } catch (InterruptedException ex) {
          ex.printStackTrace();
          currentThread().interrupt();
        }
        continue;
      }

      Set<SelectionKey> selectedKeys = selector.selectedKeys();

      if (readyChannels == 0 && selectedKeys.isEmpty()) {
        continue;
      }

      Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

      while (keyIterator.hasNext()) {
        handleSelectionKey(keyIterator);
      }
    }

    Log.putStd("ConnectionHandler thread died...");
  }

  private void readPeersBackToSelector() {
    Peer peer;

    while ((peer = doneRead.poll()) != null) {
      finishedReadingPeer(peer);
    }
  }

  private void handleSelectionKey(Iterator<SelectionKey> keyIterator) {
    SelectionKey key = keyIterator.next();
    keyIterator.remove();
    if (!key.isValid()) {
      Log.putStd("key was invalid");
      key.cancel();
      return;
    }

    try {

      if (key.isAcceptable()) {
        keyAccept(key);
        return;
      } else if (key.attachment() instanceof PeerInHandshake) {
        handlePeerInHandshake(key);
        return;
      }

      if (checkKeyAndAttachment(key)) {
        return;
      }

      if (key.isConnectable()) {
        handleKeyConnectable(key);
      } else if (key.isReadable()) {
        handleKeyReadable(key);
      } else if (key.isWritable()) {
        handleKeyWriteable(key);
      }

    } catch (IOException e) {
      key.cancel();
      if (key.attachment() instanceof PeerInHandshake) {
        PeerInHandshake peerInHandshake = (PeerInHandshake) key.attachment();
        Log.putStd("error! " + peerInHandshake.ip);
        try {
          peerInHandshake.getSocketChannel().close();
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      } else if (key.attachment() instanceof Peer) {
        Peer peer = (Peer) key.attachment();
        Log.putStd("error! " + peer.ip);
        peer.disconnect("IOException");
      }

      e.printStackTrace();
    } catch (Exception e) {
      key.cancel();
      e.printStackTrace();
      Log.sentry(e);
    }
  }

  private boolean checkKeyAndAttachment(SelectionKey key) {
    Peer peer = (Peer) key.attachment();
    if (peer == null) {
      key.cancel();
      return true;
    }

    if (!key.isValid()) {
      peer.disconnect("key is invalid.");
      return true;
    }
    return false;
  }

  private boolean handleKeyWriteable(SelectionKey key) {
    Peer peer = (Peer) key.attachment();
    peer.writeBufferLock.lock();
    try {

      int writtenBytes = 0;
      boolean remainingBytes = true;

      /**
       * First encrypt all bytes from the writebuffer to the writebuffercrypted... todo: this should
       * be done in a seperate thread/threadpool...
       */
      peer.encrypteOutputdata();

      peer.writeBufferCrypted.flip();
      remainingBytes = peer.writeBufferCrypted.hasRemaining();
      peer.writeBufferCrypted.compact();

      // switch buffer for reading
      if (!remainingBytes) {
        key.interestOps(SelectionKey.OP_READ);
      } else {
        try {
          writtenBytes = peer.writeBytesToPeer();
          Log.put("written bytes: " + writtenBytes, 200);
        } catch (IOException e) {
          e.printStackTrace();
          Log.putStd("could not write bytes to peer, peer disconnected?");
          peer.disconnect("could not write");
          return true;
        }
      }

      Server.addOutBytes(writtenBytes);
      peer.sendBytes += writtenBytes;
    } finally {
      peer.writeBufferLock.unlock();
    }
    return false;
  }

  private void handleKeyReadable(SelectionKey key) {
    Peer peer = (Peer) key.attachment();
    int interestOps = key.interestOps();

    if (interestOps == (SelectionKey.OP_WRITE | SelectionKey.OP_READ)) {
      key.interestOps(SelectionKey.OP_WRITE);
    } else if (interestOps == SelectionKey.OP_READ) {
      key.interestOps(0);
    } else {
      System.out.println("Error code 45354824173 " + interestOps);
      key.interestOps(0);
    }

    if (!workingRead.contains(peer)) {
      workingRead.add(peer);
      peersToReadAndParse.add(peer);
    } else {
      Log.putStd(
          "Error code 1429172674 "
              + workingRead.size()
              + " "
              + doneRead.size()
              + " "
              + peersToReadAndParse.size());
    }
  }

  private boolean handleKeyConnectable(SelectionKey key) {
    Peer peer = (Peer) key.attachment();
    boolean connected = false;
    try {
      connected = peer.getSocketChannel().finishConnect();
    } catch (IOException | SecurityException e) {
      e.printStackTrace();
    }

    if (!connected) {
      Log.put("connection could not be established...", 150);
      key.cancel();
      peer.disconnect("connection could not be established");
      return true;
    }

    Log.putStd("Connection established...");
    key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    return false;
  }

  private void keyAccept(SelectionKey key) throws IOException {
    // a connection was accepted by a ServerSocketChannel.
    if (!Settings.NAT_OPEN) {
      Settings.NAT_OPEN = true;
    }

    ServerSocketChannel s = (ServerSocketChannel) key.channel();
    SocketChannel socketChannel = s.accept();
    socketChannel.configureBlocking(false);
    String ip = socketChannel.socket().getInetAddress().getHostAddress();

    Log.put("incoming connection from ip: " + ip, 12);

    try {
      socketChannel.configureBlocking(false);

      selector.wakeup();
      SelectionKey newKey = socketChannel.register(selector, SelectionKey.OP_READ);

      PeerInHandshake peerInHandshake = new PeerInHandshake(ip, socketChannel);
      addPeerInHandshake(peerInHandshake);

      ConnectionReaderThread.sendHandshake(serverContext, peerInHandshake);

      newKey.attach(peerInHandshake);
      peerInHandshake.setKey(newKey);
      selector.wakeup();
    } catch (IOException ex) {
      ex.printStackTrace();
      Log.putStd("could not init connection....");
    }
  }

  public void addPeerInHandshake(PeerInHandshake peerInHandshake) {
    peerInHandshakesLock.lock();
    try {
      peerInHandshakes.add(peerInHandshake);
    } finally {
      peerInHandshakesLock.unlock();
    }
  }

  private void finishedReadingPeer(Peer p) {
    try {
      workingRead.remove(p);

      // ToDo: optimize
      p.writeBufferLock.lock();
      try {
        p.setWriteBufferFilled();
      } finally {
        p.writeBufferLock.unlock();
      }

      p.getSelectionKey().interestOps(p.getSelectionKey().interestOps() | SelectionKey.OP_READ);

    } catch (CancelledKeyException e) {
      Log.putStd("key was canneled");
    }
  }

  private void handlePeerInHandshake(SelectionKey key) {
    try {

      PeerInHandshake peerInHandshake = (PeerInHandshake) key.attachment();

      if (key.isConnectable()) {
        boolean connected = false;
        try {
          connected = peerInHandshake.getSocketChannel().finishConnect();
        } catch (IOException | SecurityException ignored) {
          // connection could not be established, will be handled by !connected check
        }

        if (!connected) {
          Log.put("connection could not be established...", 150);
          key.cancel();
          return;
        }

        // we have to remove the OP_CONNECT interest or the selection key will be faulty
        key.interestOps(SelectionKey.OP_READ);

        Log.putStd("Connection established...");
        ConnectionReaderThread.sendHandshake(serverContext, peerInHandshake);
      }
      if (key.isReadable()) {

        /** Lets read that data from the other Peer. */
        ByteBuffer allocate = ByteBuffer.allocate(117);
        int read = peerInHandshake.getSocketChannel().read(allocate);
        if (read == -1) {
          System.out.println("peer disconnected...");
          key.cancel();
          return;
        } else if (read == 0) {
          System.out.println("read zero bytes...");
          return;
        }

        allocate.flip();

        if (!peerInHandshake.isEncryptionActive()) {

          Log.put("read: " + read + " " + key.interestOps(), 150);
          if (peerInHandshake.getStatus() == 0) {
            /** The status indicates that no handshake was parsed before for this PeerInHandshake */
            ConnectionReaderThread.parseHandshake(serverContext, peerInHandshake, allocate);
          } else {

            /**
             * The status indicates that the first handshake was already parsed before for this
             * PeerInHandshake. Here we are providing more data for the other Peer like the public
             * key.
             */
            byte command = allocate.get();
            if (command == Command.REQUEST_PUBLIC_KEY) {
              /** The other Peer requested our public key, lets send our public key! */
              ConnectionReaderThread.sendPublicKeyToPeer(serverContext, peerInHandshake);
            } else if (command == Command.SEND_PUBLIC_KEY && peerInHandshake.getStatus() == 1) {
              /**
               * We got the public key of the Peer, lets store it and check that this public key
               * indeed corresponds to the KademliaId. v23: 64-byte Ed25519/X25519 export; v22
               * (legacy light clients): 65-byte brainpool key.
               */
              if (peerInHandshake.isProtocolV23()) {
                byte[] bytesPublicKey = new byte[NodeId.PUBLIC_KEYLEN];
                allocate.get(bytesPublicKey);

                NodeId nodeId = NodeId.importPublic(bytesPublicKey);

                Log.put("new nodeid from peer: " + nodeId.getKademliaId(), 20);

                if (!peerInHandshake.getIdentity().equals(nodeId.getKademliaId())) {
                  /**
                   * We obtained a public key which does not match the KademliaId of this Peer and
                   * should cancel that connection here.
                   */
                  Log.put("Wrong KademliaId/Public Key for that peer...", 20);
                  peerInHandshake.setStatus(2);
                  peerInHandshake.getSocketChannel().close();
                } else {
                  /**
                   * We obtained the correct public key and can add it to the Peer and lets set that
                   * peerInHandshake status to waiting for encryption
                   */
                  peerInHandshake.getPeer().setNodeId(nodeId);
                  peerInHandshake.setNodeId(nodeId);
                  peerInHandshake.setStatus(-1);
                }
              } else {
                byte[] bytesPublicKey =
                    new byte[im.redpanda.crypt.legacy.LegacyNodeId.PUBLIC_KEYLEN];
                allocate.get(bytesPublicKey);

                im.redpanda.crypt.legacy.LegacyNodeId legacyNodeId =
                    im.redpanda.crypt.legacy.LegacyNodeId.importPublic(bytesPublicKey);

                Log.put("new legacy (v22) nodeid from peer: " + legacyNodeId.getKademliaId(), 20);

                if (!peerInHandshake.getIdentity().equals(legacyNodeId.getKademliaId())) {
                  Log.put("Wrong KademliaId/Public Key for that legacy peer...", 20);
                  peerInHandshake.setStatus(2);
                  peerInHandshake.getSocketChannel().close();
                } else {
                  peerInHandshake.setLegacyNodeIdFromThem(legacyNodeId);
                  // the Peer only keeps the KademliaId — the legacy key is never used outside
                  // the v22 handshake
                  peerInHandshake.getPeer().setNodeId(new NodeId(legacyNodeId.getKademliaId()));
                  peerInHandshake.setStatus(-1);
                }
              }

            } else if (command == Command.ACTIVATE_ENCRYPTION) {

              /**
               * We received the byte to activate the encryption. v23: the payload is the 32-byte
               * ephemeral X25519 public key of the peer; v22 (legacy): 8 random bytes for the
               * AES-CTR IV.
               */
              if (peerInHandshake.isProtocolV23()) {
                if (allocate.remaining() < 32) {
                  System.out.println("not enough bytes for encryption... " + allocate.remaining());
                  peerInHandshake.getSocketChannel().close();
                  return;
                }
                byte[] ephemeralFromThem = new byte[32];
                allocate.get(ephemeralFromThem);
                peerInHandshake.setEphemeralPublicFromThem(ephemeralFromThem);
              } else {
                if (allocate.remaining() < PeerInHandshake.IVbytelen / 2.) {
                  System.out.println("not enough bytes for encryption... " + allocate.remaining());
                  peerInHandshake.getSocketChannel().close();
                  return;
                }
                byte[] randomBytesFromThem = new byte[PeerInHandshake.IVbytelen / 2];
                allocate.get(randomBytesFromThem);
                peerInHandshake.setRandomFromThem(randomBytesFromThem);
              }

              peerInHandshake.setAwaitingEncryption(true);

              System.out.println("parsed ACTIVATE_ENCRYPTION");
            }
          }

          /** Lets check if we are ready to start the encryption for this handshaking peer */
          if (peerInHandshake.getStatus() == -1 && !peerInHandshake.isWeSendOurRandom()) {
            byte[] keyMaterialFromUs =
                peerInHandshake.isProtocolV23()
                    ? peerInHandshake.getEphemeralPublicFromUs()
                    : peerInHandshake.getRandomFromUs();
            ByteBuffer activateEncryptionBuffer = ByteBuffer.allocate(1 + keyMaterialFromUs.length);
            activateEncryptionBuffer.put(Command.ACTIVATE_ENCRYPTION);

            activateEncryptionBuffer.put(keyMaterialFromUs);

            activateEncryptionBuffer.flip();

            long write = peerInHandshake.getSocketChannel().write(activateEncryptionBuffer);
            Log.put("written bytes for ACTIVATE_ENCRYPTION: " + write, 100);
            peerInHandshake.setWeSendOurRandom(true);
          }

          if (peerInHandshake.getStatus() == -1
              && peerInHandshake.isAwaitingEncryption()
              && peerInHandshake.hasPublicKey()) {
            peerInHandshake.setAwaitingEncryption(false);

            Log.put("lets generate the shared secret", 80);

            peerInHandshake.calculateSharedSecret(serverContext);

            /**
             * Shared Secret and IV calculated via ECDH and random bytes, lets activate the
             * encryption
             */
            peerInHandshake.activateEncryption();

            /** lets send the first ping */
            ByteBuffer bytesSendToPing = ByteBuffer.allocate(1);
            bytesSendToPing.put(Command.PING);
            bytesSendToPing.flip();

            // a v23 GCM frame for a single byte needs 33 bytes, so borrow a bit more
            ByteBuffer byteBuffer = ByteBufferPool.borrowObject(64);

            peerInHandshake.getPeerChiperStreams().encrypt(bytesSendToPing, byteBuffer);

            byteBuffer.flip();

            try {
              int write = peerInHandshake.getSocketChannel().write(byteBuffer);
              Log.put("written bytes for PING: " + write, 80);
            } catch (IOException e) {
              e.printStackTrace();
            }

            byteBuffer.compact();
            ByteBufferPool.returnObject(byteBuffer);

            /**
             * REDPANDAJ-2DS: if the peer's ACTIVATE_ENCRYPTION and its first GCM frame (counter 0)
             * were coalesced by the kernel into this single read(), 'allocate' still holds the
             * ciphertext of that first frame after the plaintext branch above consumed only the
             * ACTIVATE_ENCRYPTION command. Encryption is active now (activateEncryption() just
             * ran), so feed the leftover bytes into decrypt() right away instead of silently
             * dropping them - dropping them would desync the receive counter and the next frame
             * would fail with "unexpected GCM frame nonce".
             */
            if (allocate.hasRemaining()) {
              handleFirstEncryptedCommand(peerInHandshake, allocate);
            }
          }

        } else {
          /** The encryption is active in this section, lets check that first ping */
          handleFirstEncryptedCommand(peerInHandshake, allocate);
        }
      }

    } catch (IOException e) {
      Log.put("caught io exception in handshake...", 20);
      key.cancel();
    } catch (Exception e) {
      Log.put("Handshake failed with throwable...", 5);
      Log.sentry(e);
      key.cancel();
    }
  }

  /**
   * Decrypts one ciphertext buffer expected to contain the peer's first GCM frame (counter 0) and
   * finishes the handshake if it is a PING. Used both for the normal case (a dedicated read event
   * with encryption already active) and for leftover bytes that arrived coalesced with
   * ACTIVATE_ENCRYPTION in the same read() (REDPANDAJ-2DS).
   */
  private void handleFirstEncryptedCommand(PeerInHandshake peerInHandshake, ByteBuffer cipherText)
      throws IOException, PeerProtocolException {
    System.out.println("received first encrypted command...");

    ByteBuffer tempHandshakeReadBuffer = ByteBufferPool.borrowObject(64);

    try {
      peerInHandshake.getPeerChiperStreams().decrypt(cipherText, tempHandshakeReadBuffer);

      tempHandshakeReadBuffer.flip();

      if (!tempHandshakeReadBuffer.hasRemaining()) {
        // the frame's ciphertext was not fully available yet (e.g. split across reads);
        // GcmFramedStreams buffers it internally and will decode it once the rest arrives.
        return;
      }

      byte decryptedCommand = tempHandshakeReadBuffer.get();

      if (decryptedCommand == Command.PING) {
        System.out.println("received first ping...");

        /**
         * We can now safely transfer the open connection from the peerInHandshake to the actual
         * peer
         */
        Peer peer = peerInHandshake.getPeer();
        setupConnection(peer, peerInHandshake);

        copyRemainingReadBytesToPeerBuffer(tempHandshakeReadBuffer, peer);
      } else {
        System.out.println("got wrong first command, lets disconnect");
        peerInHandshake.getSocketChannel().close();
      }
    } finally {
      // ByteBufferPool.returnObject requires position == 0 && limit == capacity, or it
      // invalidates the buffer (destroying it + logging a Sentry warning) instead of pooling it.
      // The early return above (no complete frame yet - an expected, regular occurrence, not an
      // error) left the buffer flipped but not compacted, which would otherwise trip that check
      // on every such read; clear() normalizes every exit path (including exceptions) in one place
      // instead of relying on a compact() call before each return.
      tempHandshakeReadBuffer.clear();
      ByteBufferPool.returnObject(tempHandshakeReadBuffer);
    }
  }

  private void copyRemainingReadBytesToPeerBuffer(ByteBuffer tempHandshakeReadBuffer, Peer peer) {
    if (tempHandshakeReadBuffer.hasRemaining()) {
      if (peer.readBuffer == null) {
        peer.readBuffer = ByteBufferPool.borrowObject(tempHandshakeReadBuffer.remaining());
      }
      peer.readBuffer.put(tempHandshakeReadBuffer);
    }
  }

  public void setupConnection(Peer peerOrigin, PeerInHandshake peerInHandshake) {

    ReentrantLock writeBufferLock = peerOrigin.getWriteBufferLock();
    writeBufferLock.lock();

    try {
      removePeerInHandshake(peerInHandshake);

      peerOrigin.setupConnectionForPeer(peerInHandshake);

      // update the selection key to the actual peer
      peerInHandshake.getKey().attach(peerOrigin);

      // Log a clear success message for e2e and operators
      logger.info(
          "Connected successfully to {}:{} (KadId: {})",
          peerOrigin.getIp(),
          peerOrigin.getPort(),
          peerInHandshake.getIdentity());

      /**
       * If this is a new connection not initialzed by us this peer might not be in our PeerList,
       * lets add it by KademliaId
       */
      Peer oldPeer = peerList.add(peerOrigin);
      if (oldPeer != null && oldPeer.isConnected()) {
        if (!peerOrigin.getNodeId().equals(oldPeer.getNodeId())) {
          System.out.println("already connected to same node with same id");
        } else {
          System.out.println("already connected to same node with same ip+port");
        }
      }

      /** Lets search for the Node object for that peer and load it. */
      if (!peerInHandshake.isLightClient()) {
        Node byKademliaId = Node.getByKademliaId(serverContext, peerInHandshake.getIdentity());
        if (byKademliaId == null) {
          byKademliaId = new Node(serverContext, peerInHandshake.getNodeId());
        } else {
          System.out.println(
              "found node in db: "
                  + byKademliaId.getNodeId().getKademliaId()
                  + " last seen: "
                  + Utils.formatDuration(System.currentTimeMillis() - byKademliaId.getLastSeen()));
        }
        byKademliaId.seen(peerInHandshake.ip, peerInHandshake.getPort());
        peerOrigin.setNode(byKademliaId);
      }

    } finally {
      writeBufferLock.unlock();
    }
  }

  public void removePeerInHandshake(PeerInHandshake peerInHandshake) {
    peerInHandshakesLock.lock();
    try {
      peerInHandshakes.remove(peerInHandshake);
    } finally {
      peerInHandshakesLock.unlock();
    }
  }
}
