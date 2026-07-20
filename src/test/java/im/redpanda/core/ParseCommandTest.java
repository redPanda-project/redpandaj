package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.proto.PeerInfoProto;
import im.redpanda.proto.SendPeerList;
import java.nio.ByteBuffer;
import java.security.Security;
import org.junit.Test;

public class ParseCommandTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    ByteBufferPool.init();
  }

  public Peer getPeerForDebug() {
    Peer me = new Peer("me", 1);
    me.setNodeId(new NodeId());
    me.writeBuffer = ByteBuffer.allocate(1024 * 1024 * 5);
    return me;
  }

  @Test
  public void testLoopCommands() {

    ServerContext serverContext = new ServerContext();
    InboundCommandProcessor processor = new InboundCommandProcessor(serverContext);

    // lets check if it is able to parse 3 ping commands in one step
    ByteBuffer allocate = ByteBuffer.allocate(1024);
    allocate.put(Command.PING);
    allocate.put(Command.PING);
    allocate.put(Command.PING);

    Peer peerForDebug = getPeerForDebug();
    serverContext.getPeerList().add(peerForDebug);
    peerForDebug.setConnected(true);
    // loopCommands only compacts the buffer it still owns via peer.readBuffer (REDPANDAJ-2DR
    // hardening: a handler that disconnects the peer already returns/nulls peer.readBuffer, so
    // compacting a stale local reference afterwards would corrupt a buffer some other peer may
    // already have borrowed from the pool). Wire it up the same way ConnectionReaderThread does.
    peerForDebug.readBuffer = allocate;
    processor.loopCommands(peerForDebug, allocate);

    // lets go to read mode and check for remaining bytes
    allocate.flip();
    assertThat(allocate.hasRemaining()).isFalse();

    // lets check a not complete SEND PEERLIST command
    allocate = ByteBuffer.allocate(1024);
    allocate.put(Command.SEND_PEERLIST);
    allocate.putInt(1);

    peerForDebug.readBuffer = allocate;
    processor.loopCommands(peerForDebug, allocate);

    // lets go to read mode and check for remaining bytes
    allocate.flip();

    assertThat(allocate.get()).isEqualTo(Command.SEND_PEERLIST);
    assertThat(allocate.getInt()).isEqualTo(1);

    // lets combine both from above
    allocate = ByteBuffer.allocate(1024);
    allocate.put(Command.PING);
    allocate.put(Command.PING);
    allocate.put(Command.PING);
    allocate.put(Command.PING);
    allocate.put(Command.SEND_PEERLIST);
    allocate.putInt(1);

    peerForDebug.setConnected(true);
    peerForDebug.readBuffer = allocate;
    processor.loopCommands(peerForDebug, allocate);

    // lets go to read mode and check for remaining bytes
    allocate.flip();

    assertThat(allocate.get()).isEqualTo(Command.SEND_PEERLIST);
    assertThat(allocate.getInt()).isEqualTo(1);
  }

  /**
   * REDPANDAJ-2DR (Copilot review finding): {@code loopCommands}'s {@code finally} block must not
   * blindly {@code compact()} the buffer once a handler has disconnected the peer mid-loop. {@link
   * Peer#disconnect(String)} already resets the buffer to (position=0, limit=capacity) and returns
   * that exact instance to {@link ByteBufferPool} — potentially to a different peer/thread already
   * — so compacting it afterwards would corrupt whatever state the new owner sees. This drives a
   * real disconnecting handler (negative update content length) through the actual loop instead of
   * asserting on the guard directly.
   */
  @Test
  public void loopCommands_doesNotTouchBufferAfterHandlerDisconnectsPeer() {
    ServerContext serverContext = new ServerContext();
    InboundCommandProcessor processor = new InboundCommandProcessor(serverContext);

    ByteBuffer buffer = ByteBufferPool.borrowObject(1024);
    buffer.put(Command.UPDATE_ANSWER_CONTENT);
    buffer.putLong(Updater.MIN_UPDATE_TIMESTAMP_MS + 1_000_000L);
    buffer.putInt(-1); // network-controlled negative length -> handler disconnects the peer
    buffer.put(new byte[NodeId.SIGNATURE_LEN]);

    Peer peer = getPeerForDebug();
    serverContext.getPeerList().add(peer);
    peer.setConnected(true);
    peer.readBuffer = buffer;

    processor.loopCommands(peer, buffer);

    assertThat(peer.isConnected()).isFalse();
    assertThat(peer.readBuffer)
        .as("Peer.disconnect() must have returned the buffer, clearing the field")
        .isNull();
    // If loopCommands had wrongly compact()-ed the buffer after disconnect() already reset and
    // returned it, position would be pulled up towards capacity instead of staying at 0.
    assertThat(buffer.position()).isEqualTo(0);
    assertThat(buffer.limit()).isEqualTo(buffer.capacity());
  }

  /**
   * REDPANDAJ-2E0: an unknown command byte (observed as command {@code 0} right after another
   * command, i.e. a stream desync) used to throw a {@code RuntimeException} that was only logged
   * while the peer stayed connected and kept re-hitting the same desynced byte on every read.
   * {@code parseCommand} must now disconnect the peer instead, so the connection re-handshakes and
   * resyncs. Drives a real unknown command through the actual loop.
   */
  @Test
  public void loopCommands_disconnectsPeerOnUnknownCommand() {
    ServerContext serverContext = new ServerContext();
    InboundCommandProcessor processor = new InboundCommandProcessor(serverContext);

    ByteBuffer buffer = ByteBufferPool.borrowObject(1024);
    buffer.put((byte) 0); // command 0 is not a registered command -> protocol desync

    Peer peer = getPeerForDebug();
    serverContext.getPeerList().add(peer);
    peer.setConnected(true);
    peer.readBuffer = buffer;

    processor.loopCommands(peer, buffer);

    assertThat(peer.isConnected())
        .as("an unknown command must disconnect the peer, not leave it connected and desynced")
        .isFalse();
    assertThat(peer.readBuffer)
        .as("Peer.disconnect() must have returned the buffer, clearing the field")
        .isNull();
  }

  @Test
  public void testREQUEST_PEERLIST() {
    ServerContext serverContext = new ServerContext();
    InboundCommandProcessor processor = new InboundCommandProcessor(serverContext);
    PeerList peerList = serverContext.getPeerList();

    int peersToTest = 100;

    int startingPeerListSize = peerList.size();

    int i = 0;
    for (i = 0; i < peersToTest; i++) {
      Peer testpeer1 = new Peer("rand_rewrewR_testip" + i, i);
      testpeer1.setNodeId(new NodeId());
      testpeer1.setConnected(true);
      peerList.add(testpeer1);
    }

    Peer me = getPeerForDebug();

    processor.parseCommand(Command.REQUEST_PEERLIST, null, me);

    ByteBuffer writeBuffer = me.getWriteBuffer();

    // System.out.println("" + writeBuffer);

    writeBuffer.flip();

    byte cmd = writeBuffer.get();

    assertThat(cmd).isEqualTo(Command.SEND_PEERLIST);

    int bytesforBuffer = writeBuffer.getInt();

    byte[] bytesForProtoPeerList = new byte[bytesforBuffer];

    writeBuffer.get(bytesForProtoPeerList);

    try {
      SendPeerList sendPeerList = SendPeerList.parseFrom(bytesForProtoPeerList);
      int peerListSize = sendPeerList.getPeersCount();
      assertThat(peerListSize).isEqualTo(peerList.size());

      // Check content of a few entries
      for (int k = 0; k < peerListSize; k++) {
        PeerInfoProto peerProto = sendPeerList.getPeers(k);
        // Note: The order isn't guaranteed to be strictly predictable unless we sort,
        // but for this test setup
        // the peerList implementation might return them in order or not.
        // However, the test logic was building peers with ip "rand_rewrewR_testip" + i
        // Let's verify that the ip matches the pattern or exists in our set
        // For simplicity, we just assert the structure is valid.
        assertThat(peerProto.getIp()).contains("testip");
        assertThat(peerProto.getPort()).isGreaterThanOrEqualTo(0);
        if (peerProto.hasNodeId()) {
          assertThat(peerProto.getNodeId().getPublicKeyBytes().size())
              .isEqualTo(NodeId.PUBLIC_KEYLEN);
        }
      }

    } catch (InvalidProtocolBufferException e) {
      org.junit.Assert.fail("Failed to parse SendPeerList protobuf: " + e.getMessage());
    }

    assertThat(writeBuffer.remaining()).isZero();

    // cleanup
    for (i = 0; i < peersToTest; i++) {
      peerList.removeIpPort("rand_rewrewR_testip" + i, i);
    }

    assertThat(startingPeerListSize).isEqualTo(peerList.size());
  }

  @Test
  public void testSend_PEERLIST() {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    InboundCommandProcessor processor = new InboundCommandProcessor(serverContext);
    PeerList peerList = serverContext.getPeerList();

    int peersToTest = 100;

    int initPeerListSize = peerList.size();

    int i = 0;
    for (i = 0; i < peersToTest; i++) {
      Peer testpeer1 = new Peer("rand_dwhrgfwer_testip" + i, i);
      testpeer1.setNodeId(new NodeId());
      testpeer1.setConnected(true);
      // System.out.println("node id: " +
      // testpeer1.getNodeId().getKademliaId().toString());
      peerList.add(testpeer1);
    }
    // PeerList.getReadWriteLock().writeLock().unlock();

    Peer me = getPeerForDebug();

    processor.parseCommand(Command.REQUEST_PEERLIST, null, me);

    // PeerList.getReadWriteLock().writeLock().lock();

    peerList.clear();

    ByteBuffer writeBuffer = me.getWriteBuffer();

    writeBuffer.flip();

    // prints bytes of flatbuffer object to console...
    byte cmd = writeBuffer.get();
    assertThat(cmd).isEqualTo(Command.SEND_PEERLIST);

    int toreadbytes = writeBuffer.getInt();
    byte[] bytes = new byte[toreadbytes];
    writeBuffer.get(bytes);

    // Feed it back
    ByteBuffer readBuffer = ByteBuffer.allocate(1 + 4 + bytes.length);
    readBuffer.put(cmd);
    readBuffer.putInt(toreadbytes);
    readBuffer.put(bytes);
    readBuffer.flip();
    readBuffer.get(); // consume command byte

    processor.parseCommand(cmd, readBuffer, getPeerForDebug());

    assertThat(readBuffer.hasRemaining()).isFalse();

    assertThat(peerList.size() - initPeerListSize).isEqualTo(peersToTest);
  }
}
