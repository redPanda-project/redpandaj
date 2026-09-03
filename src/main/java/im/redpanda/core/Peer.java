/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;

import im.redpanda.core.exceptions.PeerProtocolException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import lombok.Getter;

/**
 * One connection to one remote node — the aggregate root of the transport context (DDD review
 * §3/§6, N-TRANSPORT).
 *
 * <h2>Write path (T115)</h2>
 *
 * <p>The write buffers and the lock that owns them are internal to this package. Everything outside
 * {@code im.redpanda.core} queues bytes through the small frame API — {@link
 * #enqueueCommand(byte)}, {@link #enqueueFrame(byte, byte[])}, {@link #tryEnqueueFrame(byte,
 * byte[], long, TimeUnit)} and {@link #enqueueGrowingFrame(ByteBuffer)} (plus the package-private
 * {@link #writeBufferLocked(Consumer)} for the two frames that are not length-prefixed) — all of
 * which take {@link #writeBufferLock} themselves, re-read and null-check {@code writeBuffer} under
 * it, release it in a {@code finally} and then register the peer for writing.
 *
 * <p>That contract used to be a convention repeated at ~15 call sites in the routing, mailbox, DHT
 * and updater code ("lock, re-read the field, null-check, unlock in a finally"), and several sites
 * got a part of it wrong: missing {@code finally} (leaving {@code writeBufferLock} held forever
 * after an NPE, bug hunt L4), a missing null re-read (TD008/REDPANDAJ-2EJ) or a buffer replacement
 * done from outside the class ({@code InboundCommandProcessor.appendToWriteBuffer}). The lock
 * discipline now lives in exactly one place: here.
 *
 * @author rflohr
 */
public class Peer implements Comparable<Peer> {

  private Node node;
  String ip;
  int port;
  int connectAble = 0;

  private boolean lightClient = false;
  int protocolVersion;

  int retries = 0;
  @Getter private long lastPongReceived = 0;
  int cnt = 0;
  long connectedSince = 0;
  private NodeId nodeId;
  // volatile: readConnection()'s stale-connection guards compare these lock-free against a
  // captured reference while setupConnectionForPeer() swaps them under writeBufferLock — without
  // volatile the JMM permits a stale read, making a guard misfire and tear down the fresh
  // replacement connection (REDPANDAJ-2EF review finding).
  private volatile SocketChannel socketChannel;
  ByteBuffer readBuffer;
  ByteBuffer writeBuffer;
  volatile SelectionKey selectionKey;
  private boolean connected = false;
  boolean isConnecting;
  long lastPinged = 0;
  double ping = 0;
  private boolean authed = false;
  ByteBuffer writeBufferCrypted;
  final ReentrantLock writeBufferLock = new ReentrantLock();
  Thread connectingThread;
  ArrayList<Integer> removedSendMessages = new ArrayList<>();
  byte lastCommand;

  long sendBytes = 0;
  long receivedBytes = 0;

  boolean isConnectionInitializedByMe = false;

  private boolean isIntegrated = false;

  // new variables since redpanda2.0
  private PeerChiperStreams peerChiperStreams;

  public Peer(String ip, int port) {
    this.ip = ip;
    this.port = port;
  }

  public Peer(String ip, int port, NodeId id) {
    this.ip = ip;
    this.port = port;
    this.nodeId = id;
  }

  /**
   * Set the nodeId of this Peer, does not check the consitency with the KademliaId.
   *
   * @param nodeId
   */
  public void setNodeId(NodeId nodeId) {
    this.nodeId = nodeId;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public void clearNode() {
    this.node = null;
  }

  public void setNode(Node node) {

    if (this.nodeId != null && !this.nodeId.equals(node.getNodeId())) {
      System.out.println(
          "set wrong node to peer, panic: %s - %s".formatted(this.nodeId, node.getNodeId()));
    }

    this.node = node;
  }

  public Node getNode() {
    if (!isAuthed() || !connected) {
      return null;
    }
    return node;
  }

  public KademliaId getKademliaId() {
    if (getNodeId() == null) {
      return null;
    }
    return getNodeId().getKademliaId();
  }

  public boolean equalsIpAndPort(Object obj) {

    if (obj instanceof Peer n2) {

      return ip.equals(n2.ip) && port == n2.port;

    } else {
      return false;
    }
  }

  public boolean equalsNonce(Object obj) {

    if (obj instanceof Peer n2) {

      if (getNodeId() == null
          || getNodeId().getKademliaId() == null
          || n2.getNodeId() == null
          || n2.getNodeId().getKademliaId() == null) {
        return false;
      }

      return getNodeId().getKademliaId().equals(n2.getNodeId().getKademliaId());
    } else {
      return false;
    }
  }

  public boolean equalsInstance(Object obj) {
    return super.equals(obj);
  }

  public long getLastAnswered() {
    return System.currentTimeMillis() - lastPongReceived;
  }

  public boolean isConnected() {
    return connected;
  }

  public void setConnected(boolean connected) {
    this.connected = connected;
  }

  @Override
  public int compareTo(Peer o) {
    return o.getPriority() - getPriority();
  }

  public int getPriority() {

    int score = 0;

    if (connected) {
      score += 2000;
    }

    if (getNodeId() == null) {
      score -= 1000;
    }

    if (ip != null && ip.contains(":")) {
      score += 50;
    }

    score += -retries * 200;

    if (node != null) {
      score += 5000;

      score -= node.getGmTestsFailed() * 3;
      score += node.getGmTestsSuccessful() * 5;
    }

    return score;
  }

  public SocketChannel getSocketChannel() {
    return socketChannel;
  }

  public void setSocketChannel(SocketChannel socketChannel) {
    this.socketChannel = socketChannel;
  }

  public void disconnect(String reason) {

    clearNode();
    isConnecting = false;
    authed = false;
    connectedSince = 0;
    isIntegrated = false;

    // Was previously `tryLock(2, SECONDS)` with the return value ignored: on a timeout the
    // readBuffer/writeBuffer fields below were still touched and the buffer returned to the pool
    // without actually holding the lock, racing a concurrent decryptInputData()/readConnection()
    // that *does* hold it — the same double-return / invalid-buffer-state class as
    // REDPANDAJ-2E8/2ED. None of the sections writeBufferLock guards in this class do blocking
    // I/O (buffer allocation/copy/decrypt only), so blocking here is bounded and safe.
    writeBufferLock.lock();
    try {
      Log.put("DISCONNECT: " + reason, 100);

      setConnected(false);

      if (isConnecting && connectingThread != null) {
        connectingThread.interrupt();
      }

      if (selectionKey != null) {
        selectionKey.cancel();
      }
      if (socketChannel != null) {
        if (socketChannel.isOpen()) {
          try {
            socketChannel.configureBlocking(false); // ToDo: hack
          } catch (IOException ignored) {
          }
        }

        try {
          socketChannel.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      if (readBuffer != null) {
        ByteBuffer buff = readBuffer;
        readBuffer = null;
        buff.position(0);
        buff.limit(buff.capacity());
        ByteBufferPool.returnObject(buff);
      }

      writeBuffer = null;
      writeBufferCrypted = null;
    } finally {
      writeBufferLock.unlock();
    }

    Server.triggerOutboundThread();
  }

  public void sendPing() {

    if (System.currentTimeMillis() - lastPinged < 1000) {
      return;
    }

    if (getSelectionKey() == null) {
      setConnected(false);
      return;
    }
    if (!getSelectionKey().isValid()) {
      System.out.println("selectionkey invalid11!");
      setConnected(false);
      return;
    }

    lastPinged = System.currentTimeMillis();

    if (writeBufferLock.tryLock()) {
      try {
        // Re-read (and check) writeBuffer under the lock: disconnect() nulls it under this same
        // lock, so checking it before acquiring the lock leaves a window in which the field turns
        // null between the check and the tryLock() succeeding, NPE-ing on writeBuffer.remaining()
        // below (REDPANDAJ-TD008).
        ByteBuffer buffer = writeBuffer;
        if (buffer == null) {
          setConnected(false);
        } else if (buffer.remaining() > 0) {
          // remaining(), not capacity(): capacity is the fixed 300 KiB allocation size and is
          // never 0, so the old capacity() check never actually skipped a full buffer — a put()
          // on a genuinely full buffer would have thrown BufferOverflowException instead (Copilot
          // review finding on this PR). remaining() is the free-space check the "buffer has
          // content" log message below always intended.
          buffer.put(Command.PING);
          Log.put("pinged...", 100);
        } else {
          Log.put("didnt ping, buffer has content...", 100);
        }
      } finally {
        writeBufferLock.unlock();
      }
    } else {
      Log.put("Could not lock for ping!", 50);
    }

    setWriteBufferFilled();
  }

  public SelectionKey getSelectionKey() {
    return selectionKey;
  }

  public void setSelectionKey(SelectionKey selectionKey) {
    this.selectionKey = selectionKey;
  }

  public boolean setWriteBufferFilled() {

    if (!isConnected()) {
      return false;
    }

    if (writeBuffer == null) {
      return false;
    }

    SelectionKey key = getSelectionKey();
    if (key == null) {
      return false;
    }

    if (key.isValid()) {
      ConnectionHandler.selectorLock.lock();
      try {
        key.selector().wakeup();
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        return true;
      } catch (CancelledKeyException e) {
        System.out.println("cancelled key exception");
      } finally {
        ConnectionHandler.selectorLock.unlock();
      }
    } else {
      System.out.println("key is not valid");
      disconnect("key is not valid");
    }
    return false;
  }

  public void encrypteOutputdata() {

    writeBufferLock.lock();
    try {

      if (writeBuffer == null) {
        return;
      }

      writeBuffer.flip();
      int remaining = writeBuffer.remaining();

      if (remaining == 0) {
        writeBuffer.compact();
        return;
      }

      // writebuffer in read, writeBufferCrypted in write mode
      getPeerChiperStreams().encrypt(writeBuffer, writeBufferCrypted);

      writeBuffer.compact();
    } finally {
      writeBufferLock.unlock();
    }
  }

  public int decryptInputData(ByteBuffer byteBufferToDecrypt) throws PeerProtocolException {

    writeBufferLock.lock();
    try {

      byteBufferToDecrypt.flip();
      int remaining = byteBufferToDecrypt.remaining();

      if (remaining == 0) {
        byteBufferToDecrypt.compact();
        return 0;
      }

      // framed streams may release previously buffered frame bytes in this round as well
      int maxPlaintext = remaining + getPeerChiperStreams().pendingDecryptBytes();

      if (readBuffer.remaining() < maxPlaintext) {
        int newSize = Math.min(readBuffer.position() + maxPlaintext + 1024, 1024 * 1024 * 60);
        if (newSize == readBuffer.remaining()) {
          throw new PeerProtocolException(
              "buffer could not be increased, size is %s ".formatted(newSize));
        }
        Log.put("get new readBuffer with size: %s".formatted(newSize), 5);
        ByteBuffer newBuffer = ByteBufferPool.borrowObject(newSize);

        System.arraycopy(readBuffer.array(), 0, newBuffer.array(), 0, readBuffer.array().length);
        newBuffer.position(readBuffer.position());
        readBuffer.compact();
        readBuffer.position(0);
        ByteBufferPool.returnObject(readBuffer);
        readBuffer = newBuffer;
      }

      getPeerChiperStreams().decrypt(byteBufferToDecrypt, readBuffer);

      byteBufferToDecrypt.compact();

      return remaining;
    } finally {
      writeBufferLock.unlock();
    }
  }

  int writeBytesToPeer() throws IOException {
    writeBufferCrypted.flip();
    int writtenBytes = getSocketChannel().write(writeBufferCrypted);
    Log.put(
        "written bytes to node: " + writtenBytes + " remaining: " + writeBufferCrypted.remaining(),
        100);
    writeBufferCrypted.compact();

    return writtenBytes;
  }

  public boolean peerIsHigher(ServerContext serverContext) {
    for (int i = 0; i < KademliaId.ID_LENGTH / 8; i++) {
      int compare =
          Byte.toUnsignedInt(getKademliaId().getBytes()[i])
              - Byte.toUnsignedInt(serverContext.getNonce().getBytes()[i]);
      if (compare > 0) {
        return true;
      } else if (compare < 0) {
        return false;
      }
    }
    return false;
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  /**
   * Whether this peer has connection details we could dial.
   *
   * <p>False for every peer we only ever learned from an inbound connection: the handshake carries
   * the sender's listening port, and a light client has no listening socket, so it announces port 0
   * ({@code PeerInHandshake.port} also defaults to 0). The resulting {@link Peer} keeps the remote
   * end of that socket, which is useless the moment the connection is gone. Also false after {@link
   * #removeIpAndPort()}.
   */
  public boolean isDialable() {
    return ip != null && port > 0 && port <= 65535;
  }

  public boolean isAuthed() {
    return authed;
  }

  /**
   * Only the handshake ({@link #setupConnectionForPeer(PeerInHandshake)}, {@link
   * #disconnect(String)}) and tests set this — hence package-private.
   */
  void setAuthed(boolean authed) {
    this.authed = authed;
  }

  /**
   * The lock owning {@code writeBuffer}/{@code writeBufferCrypted}/{@code readBuffer}.
   *
   * <p>Package-private on purpose (T115): the NIO plumbing in this package ({@code
   * ConnectionHandler}, {@code ConnectionReaderThread}) shares these buffers with {@code Peer} and
   * therefore shares the lock. Everyone else queues bytes via the {@code enqueue*} /{@link
   * #writeBufferLocked(Consumer)} API, which takes the lock itself.
   */
  ReentrantLock getWriteBufferLock() {
    return writeBufferLock;
  }

  /**
   * The live write buffer. Package-private: writers must go through the {@code enqueue*} API so the
   * locking (and the possible buffer replacement in {@link #enqueueGrowingFrame(ByteBuffer)}) stays
   * inside this class.
   */
  ByteBuffer getWriteBuffer() {
    return writeBuffer;
  }

  /**
   * Runs {@code writer} against this peer's write buffer while holding {@link #writeBufferLock},
   * then registers the peer for writing.
   *
   * <p>Package-private: this is the raw, unframed escape hatch, and handing it to another package
   * would allow exactly the thing the {@code enqueue*} methods exist to prevent — bytes written
   * into the stream without the {@code [cmd][len][payload]} framing, which desyncs the connection.
   * The only unframed shape on this connection is {@code [cmd][long]}, and that has its own narrow
   * public entry point, {@link #enqueueTimestamp(byte, long)} (T116: the updater moved to its own
   * package and must not get the escape hatch with it).
   *
   * <p>{@code writer} must not block and must not acquire another lock: the documented lock order
   * (see {@link PeerList}) puts {@code writeBufferLock} outermost, so anything taken inside it can
   * only be a lock that is never held while waiting for this one.
   *
   * @return {@code true} if the bytes were queued, {@code false} if the peer has no write buffer
   *     any more — i.e. it disconnected (which nulls the field under this very lock)
   */
  boolean writeBufferLocked(Consumer<ByteBuffer> writer) {
    writeBufferLock.lock();
    try {
      ByteBuffer buffer = writeBuffer;
      if (buffer == null) {
        return false;
      }
      writer.accept(buffer);
    } finally {
      writeBufferLock.unlock();
    }
    setWriteBufferFilled();
    return true;
  }

  /**
   * Queues a single command byte (a wire command without a payload, e.g. {@link Command#PONG}).
   *
   * @return {@code true} if the byte was queued, {@code false} if the peer is gone
   */
  public boolean enqueueCommand(byte command) {
    return writeBufferLocked(buffer -> buffer.put(command));
  }

  /**
   * Queues an unframed {@code [command][timestamp:8]} answer — the only shape on this connection
   * that carries a payload without the {@code [len:4]} prefix, used by the four update-timestamp
   * commands (9/10 and 13/14).
   *
   * <p>Exists so the updater package (T116) does not need the package-private {@link
   * #writeBufferLocked(Consumer)} escape hatch: this one can only ever write these nine bytes.
   *
   * @return {@code true} if the bytes were queued, {@code false} if the peer is gone
   */
  public boolean enqueueTimestamp(byte command, long timestamp) {
    return writeBufferLocked(
        buffer -> {
          buffer.put(command);
          buffer.putLong(timestamp);
        });
  }

  /**
   * Queues one length-prefixed frame — {@code [command][length:4][payload]}, the shape every
   * payload-carrying command on this connection uses.
   *
   * @return {@code true} if the frame was queued, {@code false} if the peer is gone
   */
  public boolean enqueueFrame(byte command, byte[] payload) {
    return writeBufferLocked(
        buffer -> {
          buffer.put(command);
          buffer.putInt(payload.length);
          buffer.put(payload);
        });
  }

  /**
   * Like {@link #enqueueFrame(byte, byte[])}, but gives up if the write buffer stays busy longer
   * than the caller can afford.
   *
   * <p>For the Kademlia jobs: they walk many peers within one job timeout, so a single peer whose
   * buffer is locked must cost the loop a few milliseconds, not the whole job.
   *
   * @return {@code true} if the frame was queued, {@code false} if the lock was not acquired in
   *     time or the peer is gone
   */
  public boolean tryEnqueueFrame(byte command, byte[] payload, long timeout, TimeUnit unit)
      throws InterruptedException {
    if (!writeBufferLock.tryLock(timeout, unit)) {
      return false;
    }
    try {
      ByteBuffer buffer = writeBuffer;
      if (buffer == null) {
        return false;
      }
      buffer.put(command);
      buffer.putInt(payload.length);
      buffer.put(payload);
    } finally {
      writeBufferLock.unlock();
    }
    setWriteBufferFilled();
    return true;
  }

  /**
   * Appends an already built frame, growing the write buffer if the frame does not fit.
   *
   * <p>This is the only path that <em>replaces</em> {@code writeBuffer}. It used to live in {@code
   * InboundCommandProcessor.appendToWriteBuffer} and assigned the field from outside the class; the
   * growth policy (grow to old capacity + frame + 10 MiB headroom, copy the pending bytes over) is
   * unchanged, it is just no longer a foreign write into this aggregate.
   *
   * <p>Used for the update/APK upload frames, which are megabytes large and therefore cannot be
   * sized by the fixed 300 KiB connection buffer.
   *
   * @param frame the frame to append, ready to be read (position..limit)
   * @return {@code true} if the frame was queued, {@code false} if the peer is gone
   */
  public boolean enqueueGrowingFrame(ByteBuffer frame) {
    writeBufferLock.lock();
    try {
      ByteBuffer buffer = writeBuffer;
      if (buffer == null) {
        return false;
      }
      if (buffer.remaining() < frame.remaining()) {
        ByteBuffer grown =
            ByteBuffer.allocate(buffer.capacity() + frame.remaining() + 1024 * 1024 * 10);
        buffer.flip();
        grown.put(buffer);
        writeBuffer = grown;
        buffer = grown;
      }
      // put(ByteBuffer), not put(frame.array()): the bulk-array form writes the whole backing
      // array regardless of position/limit and fails outright on a non-array-backed buffer, so it
      // does not match the remaining() capacity check above.
      buffer.put(frame);
    } finally {
      writeBufferLock.unlock();
    }
    setWriteBufferFilled();
    return true;
  }

  /**
   * Whether this connection still has bytes waiting to go out — plaintext not yet encrypted, or
   * ciphertext not yet written to the socket.
   *
   * <p>Both buffers are re-read under {@link #writeBufferLock} because {@link #disconnect(String)}
   * nulls them while holding it. A peer that is no longer connected has nothing queued by
   * definition.
   */
  public boolean hasQueuedOutboundBytes() {
    writeBufferLock.lock();
    try {
      ByteBuffer buffer = writeBuffer;
      ByteBuffer crypted = writeBufferCrypted;
      if (!isConnected() || buffer == null || crypted == null) {
        return false;
      }
      return buffer.position() > 0 || crypted.position() > 0;
    } finally {
      writeBufferLock.unlock();
    }
  }

  /**
   * Frees the 2×300 KiB write buffers of a peer that is neither connected nor connecting and has
   * been silent for longer than twice the ping timeout.
   *
   * <p>Called from the chron thread ({@code PeerJobs.runOnce()}), where it used to live and where
   * it originally ran <em>without</em> this lock — the TD029/REDPANDAJ-2EJ NPE on the selector
   * thread ({@code writeBufferCrypted} nulled between the selector's own lock acquisition and its
   * deref). The condition is deliberately re-tested under the lock: {@link
   * #setupConnectionForPeer(PeerInHandshake)} holds the same lock across the whole connection swap,
   * so a reaper queued behind it must see the reconnect and keep the fresh buffers instead of
   * tearing down a live connection.
   *
   * <p>The acquisition is an unbounded {@code lock()}, so this can park the chron thread for as
   * long as a {@code writeBufferLock} section runs — worst case the {@code
   * PEERLIST_LOCK_TIMEOUT_MS} of {@code ConnectionHandler.setupConnection()}. That is the same
   * exposure the {@code peer.disconnect("timeout")} branch in the same loop already has, and the
   * chron thread is the one thread in the system that can afford to wait.
   */
  void releaseWriteBuffersIfIdle() {
    writeBufferLock.lock();
    try {
      if (!isConnected() && !isConnecting && getLastAnswered() > Settings.pingTimeout * 2) {
        writeBuffer = null;
        writeBufferCrypted = null;
      }
    } finally {
      writeBufferLock.unlock();
    }
  }

  public boolean isIntegrated() {

    if (lightClient) {
      return false;
    }

    if (isIntegrated) {
      return true;
    }

    if (connectedSince != 0 && System.currentTimeMillis() - connectedSince > 1000L * 10L) {
      isIntegrated = true;
    }

    return false;
  }

  public PeerSaveable toSaveable() {
    return new PeerSaveable(ip, port, nodeId, retries);
  }

  public void setLastPongReceived(long lastPongReceived) {
    this.lastPongReceived = lastPongReceived;
  }

  public void setPeerChiperStreams(PeerChiperStreams peerChiperStreams) {
    this.peerChiperStreams = peerChiperStreams;
  }

  public PeerChiperStreams getPeerChiperStreams() {
    return peerChiperStreams;
  }

  public void setupConnectionForPeer(PeerInHandshake peerInHandshake) {
    // disconnect old connection if present
    disconnect("new connection for this peer");

    // The whole connection swap — state flags, buffers, socketChannel, selectionKey AND the
    // cipher streams — happens in one writeBufferLock section: a concurrently running
    // ConnectionReaderThread (readConnection/decryptInputData work under the same lock since
    // REDPANDAJ-2EF) must never observe a half-replaced connection, e.g. the new GCM cipher
    // streams combined with the old socketChannel, which desyncs the frame nonce counter
    // (REDPANDAJ-2EE) or leaks bytes between the old and the new connection. The only caller
    // (ConnectionHandler.setupConnection) already holds this lock; taking the reentrant lock
    // here as well keeps the invariant local to this class.
    ReentrantLock writeBufferLock = getWriteBufferLock();
    writeBufferLock.lock();
    try {
      setConnected(true);
      isConnecting = false;
      authed = true;
      retries = 0;
      lightClient = peerInHandshake.lightClient;
      protocolVersion = peerInHandshake.protocolVersion;
      connectedSince = System.currentTimeMillis();

      /** setup the buffers */
      try {
        writeBuffer = ByteBuffer.allocate(300 * 1024);
        writeBufferCrypted = ByteBuffer.allocate(300 * 1024);
      } catch (Exception | OutOfMemoryError e) {
        // ByteBuffer.allocate throws OutOfMemoryError (an Error, not an Exception) on genuine
        // allocation failure -- the case this handler's log message and disconnect() call are
        // actually for. A plain `catch (Exception e)` never caught it, making this defensive
        // path dead code for its real purpose (Copilot review finding on this PR).
        Log.putStd("Could not reserve enough memory for this connection. Disconnect peer...");
        disconnect("Could not reserve enough memory for this connection.");
        // Early return (REDPANDAJ-TD010): without it the code below kept running on the peer
        // disconnect() just tore down, re-populating socketChannel/selectionKey/cipherStreams
        // (and, past the lock, writing into a writeBuffer that allocation never actually produced)
        // on a peer that is now disconnected and whose buffers are null again.
        return;
      }

      // set up the peer with all data from the peerInHandshake
      setLastPongReceived(System.currentTimeMillis());

      setSocketChannel(peerInHandshake.getSocketChannel());
      setSelectionKey(peerInHandshake.getKey());

      setPeerChiperStreams(peerInHandshake.getPeerChiperStreams());
    } finally {
      writeBufferLock.unlock();
    }

    if (!peerInHandshake.lightClient) {
      writeBufferLock.lock();
      try {
        writeBuffer.put(Command.UPDATE_REQUEST_TIMESTAMP);
        writeBuffer.put(Command.ANDROID_UPDATE_REQUEST_TIMESTAMP);
        // peers will now only be requested by the RequestPeerListJob
        setWriteBufferFilled();
      } finally {
        writeBufferLock.unlock();
      }
    }
  }

  @Override
  public String toString() {
    return "Peer{" + "ip='" + ip + '\'' + ", port=" + port + '}';
  }

  public void setLightClient(boolean lightClient) {
    this.lightClient = lightClient;
  }

  public boolean isLightClient() {
    return lightClient;
  }

  /** Do not call this method directly, instead use Peerlist.clearConnectionDetails(Peer peer) */
  public void removeIpAndPort() {
    ip = null;
    port = 0;
  }

  public boolean hasNode() {
    return getNode() != null;
  }
}
