/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;

import im.redpanda.core.exceptions.PeerProtocolException;
import im.redpanda.crypt.Utils;
import java.io.IOException;
import java.net.InetAddress;
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

  /**
   * Upper bound for {@link #parsePlaintextHandshakeCommands}: the plaintext phase only ever carries
   * REQUEST_PUBLIC_KEY, SEND_PUBLIC_KEY and ACTIVATE_ENCRYPTION.
   */
  static final int MAX_PLAINTEXT_HANDSHAKE_COMMANDS_PER_READ = 3;

  /** Longest plaintext handshake command: SEND_PUBLIC_KEY plus its 64-byte key export. */
  static final int MAX_PLAINTEXT_HANDSHAKE_COMMAND_LEN = 1 + NodeId.PUBLIC_KEYLEN;

  /**
   * How long {@link #setupConnection} waits for the peer list write lock before it gives up on this
   * one connection (T87). Far longer than any legitimate hold — the list is only ever iterated or
   * snapshotted under it — so a timeout means something is stuck, and the selector thread must not
   * be the one that waits it out.
   */
  static final long PEERLIST_LOCK_TIMEOUT_MS = 5000;

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
      // offer(), not add(): the queue is bounded (600) and add() throws IllegalStateException when
      // it is full. That exception only reached handleSelectionKey's generic catch, which cancels
      // the key but never disconnects the peer — the SocketChannel stayed open and the peer stayed
      // in the peerList, i.e. we leaked a socket exactly when the node was already overloaded.
      if (!peersToReadAndParse.offer(peer)) {
        workingRead.remove(peer);
        Log.putStd(
            "read queue full ("
                + peersToReadAndParse.size()
                + "), dropping connection to "
                + peer.ip);
        key.cancel();
        peer.disconnect("read queue full");
      }
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
    ServerSocketChannel s = (ServerSocketChannel) key.channel();
    SocketChannel socketChannel = s.accept();

    // accept() returns null on a spurious selector wakeup / when another thread grabbed the
    // pending connection first. Dereferencing it (configureBlocking) used to NPE out of this
    // method into handleSelectionKey's generic catch.
    if (socketChannel == null) {
      return;
    }

    if (!Settings.NAT_OPEN) {
      Settings.NAT_OPEN = true;
    }

    setupAcceptedChannel(socketChannel);
  }

  /**
   * Takes ownership of a freshly accepted channel: registers it with the selector and starts the
   * handshake. The accepted channel is closed on every failure path — a peer that connects and
   * resets immediately (port scanners, health checkers, hostile probes) otherwise leaked one file
   * descriptor per occurrence, since the outer handler in {@link
   * #handleSelectionKey(java.util.Iterator)} only closes channels for keys that already carry a
   * {@code PeerInHandshake}/{@code Peer} attachment, which the ServerSocketChannel key never does.
   */
  void setupAcceptedChannel(SocketChannel socketChannel) {
    PeerInHandshake peerInHandshake = null;
    boolean success = false;
    try {
      socketChannel.configureBlocking(false);

      // null if the peer already reset the connection — routine for scanners, so handle it as a
      // normal outcome instead of an exception; the finally below still closes the channel
      InetAddress remoteAddress = socketChannel.socket().getInetAddress();
      if (remoteAddress == null) {
        Log.put("incoming connection was already gone before setup", 12);
        return;
      }

      String ip = remoteAddress.getHostAddress();
      Log.put("incoming connection from ip: " + ip, 12);

      selector.wakeup();
      SelectionKey newKey = socketChannel.register(selector, SelectionKey.OP_READ);

      peerInHandshake = new PeerInHandshake(ip, socketChannel);
      addPeerInHandshake(peerInHandshake);

      // may throw RuntimeException on a partial handshake write
      ConnectionReaderThread.sendHandshake(serverContext, peerInHandshake);

      newKey.attach(peerInHandshake);
      peerInHandshake.setKey(newKey);
      selector.wakeup();
      success = true;
    } catch (Exception ex) {
      ex.printStackTrace();
      Log.putStd("could not init connection....");
    } finally {
      if (!success) {
        if (peerInHandshake != null) {
          removePeerInHandshake(peerInHandshake);
        }
        try {
          socketChannel.close();
        } catch (IOException closeEx) {
          closeEx.printStackTrace();
        }
      }
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

          /**
           * The buffer the plaintext handshake commands are decoded from. Usually the read buffer
           * itself; a previous read that ended mid-command hands its tail over here
           * (REDPANDAJ-2FA), and then it is the concatenation of both.
           */
          ByteBuffer plaintext = allocate;

          boolean handshakeParsed = true;
          if (peerInHandshake.getStatus() == 0) {
            /** The status indicates that no handshake was parsed before for this PeerInHandshake */
            handshakeParsed =
                ConnectionReaderThread.parseHandshake(serverContext, peerInHandshake, allocate);
            if (handshakeParsed) {
              // parseHandshake() consumed the 30-byte handshake and compacted the buffer, so
              // whatever the peer coalesced behind it is now at the front, ready to be flipped
              // for reading. Dropping it here is the same defect as REDPANDAJ-2FA.
              allocate.flip();
            }
          }

          /**
           * For a status other than 0 the first handshake was already parsed before for this
           * PeerInHandshake. Here we are providing more data for the other Peer like the public
           * key.
           */
          if (handshakeParsed) {
            plaintext = peerInHandshake.prependPlaintextHandshakeCarry(allocate);
            if (!parsePlaintextHandshakeCommands(peerInHandshake, plaintext)) {
              return;
            }
          }

          /** Lets check if we are ready to start the encryption for this handshaking peer */
          if (peerInHandshake.getStatus() == -1 && !peerInHandshake.isWeSendOurRandom()) {
            byte[] keyMaterialFromUs = peerInHandshake.getEphemeralPublicFromUs();
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
             * were coalesced by the kernel into this single read(), 'plaintext' still holds the
             * ciphertext of that first frame after the plaintext branch above stopped at the
             * ACTIVATE_ENCRYPTION command. Encryption is active now (activateEncryption() just
             * ran), so feed the leftover bytes into decrypt() right away instead of silently
             * dropping them - dropping them would desync the receive counter and the next frame
             * would fail with "unexpected GCM frame nonce".
             */
            if (plaintext.hasRemaining()) {
              handleFirstEncryptedCommand(peerInHandshake, plaintext);
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
    } catch (PeerProtocolException e) {
      // A peer that completed the v23 handshake but then sent a malformed first GCM frame (bogus
      // length/nonce/tag, REDPANDAJ-2DX) is expected hostile-network noise (port scanners,
      // non-conformant or stale clients), not an application bug. Drop the half-open connection
      // quietly instead of reporting a Sentry error on every occurrence.
      Log.put("malformed first encrypted frame, dropping handshake: " + e.getMessage(), 20);
      key.cancel();
      if (key.attachment() instanceof PeerInHandshake pih) {
        try {
          pih.getSocketChannel().close();
        } catch (IOException ignored) {
          // already tearing down this handshake
        }
      }
    } catch (Exception e) {
      Log.put("Handshake failed with throwable...", 5);
      Log.sentry(e);
      key.cancel();
    }
  }

  /**
   * Consumes the plaintext handshake commands (REQUEST_PUBLIC_KEY / SEND_PUBLIC_KEY /
   * ACTIVATE_ENCRYPTION) that are available in {@code buffer}.
   *
   * <p>REDPANDAJ-2FA: this used to parse exactly ONE command per {@code read()} and silently
   * discard the rest of the buffer. TCP is a byte stream, so a peer that answers our
   * REQUEST_PUBLIC_KEY in the same event-loop turn in which it sends its own REQUEST_PUBLIC_KEY
   * puts both commands into a single segment. We then answered the first one and dropped its
   * 65-byte SEND_PUBLIC_KEY, so the handshake stayed in status 1 forever: neither of the {@code
   * status == -1} blocks in the caller runs, we never send our own ACTIVATE_ENCRYPTION and never
   * activate encryption — while the peer treats the connection as usable and writes requests into
   * it until its own watchdog redials. In the emulator duo E2E this cost ~60 s of dead connection
   * plus the client's 90 s ack timeout and re-send backoff, i.e. a ~130 s first delivery.
   *
   * <p>Stops at the first command it cannot fully parse instead of resyncing mid-stream, and stops
   * right after ACTIVATE_ENCRYPTION so the caller's REDPANDAJ-2DS block still sees a coalesced
   * first GCM frame as leftover ciphertext.
   *
   * @return {@code false} if the connection was torn down and the caller must stop processing this
   *     read event, {@code true} otherwise.
   */
  private boolean parsePlaintextHandshakeCommands(
      PeerInHandshake peerInHandshake, ByteBuffer buffer) throws IOException {

    // The plaintext phase knows exactly three commands, so a well-behaved peer never sends more
    // than that in one read. The cap keeps a hostile peer from turning a single 117-byte read of
    // REQUEST_PUBLIC_KEY bytes into 117 public-key writes (65 bytes each) back at it.
    int commandsLeft = MAX_PLAINTEXT_HANDSHAKE_COMMANDS_PER_READ;

    while (buffer.hasRemaining() && commandsLeft-- > 0) {
      byte command = buffer.get();

      if (command == Command.REQUEST_PUBLIC_KEY) {
        /** The other Peer requested our public key, lets send our public key! */
        ConnectionReaderThread.sendPublicKeyToPeer(serverContext, peerInHandshake);
      } else if (command == Command.SEND_PUBLIC_KEY && peerInHandshake.getStatus() == 1) {
        /**
         * We got the public key of the Peer, lets store it and check that this public key indeed
         * corresponds to the KademliaId (v23: 64-byte Ed25519/X25519 export).
         */
        if (buffer.remaining() < NodeId.PUBLIC_KEYLEN) {
          return carryOverIncompleteCommand(peerInHandshake, buffer);
        }
        byte[] bytesPublicKey = new byte[NodeId.PUBLIC_KEYLEN];
        buffer.get(bytesPublicKey);

        NodeId nodeId = NodeId.importPublic(bytesPublicKey);

        Log.put("new nodeid from peer: " + nodeId.getKademliaId(), 20);

        if (!peerInHandshake.getIdentity().equals(nodeId.getKademliaId())) {
          /**
           * We obtained a public key which does not match the KademliaId of this Peer and should
           * cancel that connection here.
           */
          Log.put("Wrong KademliaId/Public Key for that peer...", 20);
          peerInHandshake.setStatus(2);
          peerInHandshake.getSocketChannel().close();
          return false;
        }
        /**
         * We obtained the correct public key and can add it to the Peer and lets set that
         * peerInHandshake status to waiting for encryption
         */
        peerInHandshake.getPeer().setNodeId(nodeId);
        peerInHandshake.setNodeId(nodeId);
        peerInHandshake.setStatus(-1);
      } else if (command == Command.ACTIVATE_ENCRYPTION) {

        /**
         * We received the byte to activate the encryption; the payload is the 32-byte ephemeral
         * X25519 public key of the peer (v23).
         */
        if (buffer.remaining() < 32) {
          return carryOverIncompleteCommand(peerInHandshake, buffer);
        }
        byte[] ephemeralFromThem = new byte[32];
        buffer.get(ephemeralFromThem);
        peerInHandshake.setEphemeralPublicFromThem(ephemeralFromThem);

        peerInHandshake.setAwaitingEncryption(true);

        System.out.println("parsed ACTIVATE_ENCRYPTION");
        // Anything left belongs to the encrypted stream (REDPANDAJ-2DS), not to this loop.
        return true;
      } else {
        // Not a plaintext handshake command we can act on in this state. Do not try to resync on
        // the following bytes — that is how a desynced stream turns into random command dispatch.
        break;
      }
    }
    return true;
  }

  /**
   * Stashes a plaintext handshake command whose payload has not fully arrived yet, so the next read
   * can decode it (REDPANDAJ-2FA). {@code buffer} is positioned right after the command byte, which
   * is rewound back into the stash.
   *
   * <p>Before this, an incomplete SEND_PUBLIC_KEY threw {@code BufferUnderflowException} into the
   * generic catch and an incomplete ACTIVATE_ENCRYPTION closed the connection ("not enough bytes
   * for encryption..."). Both cost the peer a full redial, and the light client that hit the latter
   * during an S4 reconnect did not come back at all.
   *
   * @return always {@code true} for the "wait for more bytes" case; {@code false} only when the
   *     stash would exceed the longest plaintext command, which no conforming peer can produce.
   */
  private boolean carryOverIncompleteCommand(PeerInHandshake peerInHandshake, ByteBuffer buffer)
      throws IOException {
    buffer.position(buffer.position() - 1); // put the command byte back
    byte[] carry = new byte[buffer.remaining()];
    buffer.get(carry);
    if (carry.length > MAX_PLAINTEXT_HANDSHAKE_COMMAND_LEN) {
      // Cannot happen for a conforming peer: we only get here when the command is INcomplete, so
      // the tail is shorter than the command. Belt and braces against an unbounded stash.
      System.out.println("oversized plaintext handshake remainder... " + carry.length);
      peerInHandshake.getSocketChannel().close();
      return false;
    }
    peerInHandshake.setPlaintextHandshakeCarry(carry);
    Log.put(
        "plaintext handshake command split across reads, carrying " + carry.length + " byte(s)",
        20);
    return true;
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
    if (!tempHandshakeReadBuffer.hasRemaining()) {
      return;
    }
    // peer.readBuffer is only ever touched under writeBufferLock (claim/restore in
    // ConnectionReaderThread.readConnection, Peer.disconnect(), Peer.decryptInputData() —
    // REDPANDAJ-2EF). Take the same lock so this post-handshake write neither races a reader
    // restoring leftover bytes nor writes into a buffer a reader has just claimed.
    // Known pre-existing limitation (unchanged by T50): if the new connection's first socket
    // bytes get decrypted by a reader before this copy runs, the handshake leftovers end up
    // AFTER them in the plaintext stream — a misparse then disconnects and resyncs the
    // connection. The lock prevents structural buffer corruption, not this ordering.
    peer.getWriteBufferLock().lock();
    try {
      if (peer.readBuffer == null) {
        peer.readBuffer = ByteBufferPool.borrowObject(tempHandshakeReadBuffer.remaining());
      }
      peer.readBuffer.put(tempHandshakeReadBuffer);
    } finally {
      peer.getWriteBufferLock().unlock();
    }
  }

  /**
   * Completes a handshake and registers the peer. <b>Runs on the NIO selector thread.</b>
   *
   * <p>It holds {@code peerOrigin.writeBufferLock} across the whole body and takes the peer list
   * write lock inside it — that is the documented order (see {@link PeerList}, "Lock order"), and
   * no code may take those two the other way round.
   */
  public void setupConnection(Peer peerOrigin, PeerInHandshake peerInHandshake) {

    ReentrantLock writeBufferLock = peerOrigin.getWriteBufferLock();
    writeBufferLock.lock();

    try {
      removePeerInHandshake(peerInHandshake);

      peerOrigin.setupConnectionForPeer(peerInHandshake);

      // update the selection key to the actual peer
      peerInHandshake.getKey().attach(peerOrigin);

      /**
       * If this is a new connection not initialzed by us this peer might not be in our PeerList,
       * lets add it by KademliaId
       */
      Peer oldPeer;
      try {
        oldPeer = peerList.add(peerOrigin, PEERLIST_LOCK_TIMEOUT_MS);
      } catch (PeerList.PeerListBusyException e) {
        // T87 safety net. The selector thread is the single thread that accepts new sockets and
        // services the reads and writes of every existing connection, so parking it here does not
        // cost one connection, it takes the node off the network — which is exactly what happened
        // on 2026-07-29, where the wedged selector left the listen backlog full and the process
        // alive but unreachable. Whatever holds the peer list lock for this long is a bug of its
        // own, so make it loud rather than silent, and keep the event loop running.
        logger.error(
            "peer list lock unavailable on the selector thread, dropping the connection to {}:{}"
                + " (KadId: {})",
            peerOrigin.getIp(),
            peerOrigin.getPort(),
            peerInHandshake.getIdentity(),
            e);
        Log.sentry(e);
        peerOrigin.disconnect("peer list lock unavailable on the selector thread");
        return;
      }
      if (oldPeer != null && oldPeer != peerOrigin) {
        // TD020: two inbound connections from the same node raced. Both handshakes saw
        // peerList.get(identity) == null in ConnectionReaderThread.parseHandshake and each built
        // its own, fully connected Peer object; only the first to reach peerList.add() got
        // registered. peerOrigin is the loser here — oldPeer (the winner) already holds this
        // identity, so peerList.add() returned it without registering peerOrigin. peerOrigin is
        // connected and being read by the selector (setupConnectionForPeer already adopted its
        // socket/streams and its key is attached above) but is unreachable for outbound, because
        // peerList.get(identity) returns oldPeer — i.e. a silent, un-healing orphan that keeps a
        // socket and reader busy forever. Disconnect it so no unregistered, still-reading peer
        // object survives; the winner keeps its own connection.
        //
        // The sequential half-open reconnect (T54) does NOT reach this branch: there parseHandshake
        // found the already-registered peer and set it as peerOrigin, so peerList.add() returns
        // that very object (oldPeer == peerOrigin) and the connection swap has already happened
        // inside Peer.setupConnectionForPeer (PR #271) — that case is handled by the else-if
        // diagnostic branch below, not here.
        logger.info(
            "duplicate parallel connection from the same identity (KadId: {}); disconnecting the "
                + "unregistered duplicate",
            peerInHandshake.getIdentity());
        peerOrigin.disconnect("duplicate parallel inbound connection; identity already registered");
        return;
      }

      // Log a clear success message for e2e and operators. Placed after the TD020 duplicate check
      // so it only fires for a connection we actually keep — not for a losing parallel duplicate
      // that was just disconnected above (Copilot review, PR #276).
      logger.info(
          "Connected successfully to {}:{} (KadId: {})",
          peerOrigin.getIp(),
          peerOrigin.getPort(),
          peerInHandshake.getIdentity());

      if (oldPeer != null && oldPeer.isConnected()) {
        // TD019: diagnostics only. Reaching here means oldPeer == peerOrigin (see the TD020 branch
        // above, which handles a distinct pre-existing winner): peerList.add() returned the very
        // peer we just re-registered, i.e. a reconnect/half-open swap whose atomic channel/stream
        // replacement already happened in Peer.setupConnectionForPeer(); nothing here changes that
        // outcome. The former "same node with same id" branch was removed as dead code (T54
        // analysis): peerList.add() only ever returns a non-null oldPeer whose NodeId equals
        // peerOrigin's — either via the KademliaId hashmap hit, or via the ip+port branch that
        // returns oldPeer only in its equal-NodeId else — so a "different id" case can never reach
        // this point. Switched from System.out.println to the logger so real duplicate-connection
        // incidents are traceable. The message names the guaranteed invariant (same node identity /
        // KademliaId), not "same ip+port": peerList.add() also returns the pre-existing peer on a
        // KademliaId hashmap hit whose ip+port may differ, so an ip+port claim would mislead
        // operators (Copilot review, PR #275).
        logger.info(
            "already connected to a node with the same identity (KadId: {})",
            peerInHandshake.getIdentity());
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
