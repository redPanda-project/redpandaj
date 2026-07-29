package im.redpanda.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SaverTest {

  private static final String TEST_SAVE_DIR = "data";
  private static final String TEST_FILE = TEST_SAVE_DIR + "/peers.dat";
  private static final String TEST_TMP_FILE = TEST_FILE + ".tmp";

  @Before
  @After
  public void cleanUp() throws IOException {
    Files.deleteIfExists(Path.of(TEST_FILE));
    Files.deleteIfExists(Path.of(TEST_TMP_FILE));
  }

  @Test
  public void testSaveAndLoadPeers() {
    ArrayList<Peer> peers = new ArrayList<>();
    Peer peer1 = new Peer("127.0.0.1", 1234);
    peer1.setNodeId(new NodeId());
    peers.add(peer1);

    Peer peer2 = new Peer("192.168.1.1", 5678);
    peer2.setNodeId(new NodeId());
    peers.add(peer2);

    // Save peers
    Saver.savePeers(peers);

    // Verify file creation
    File file = new File(TEST_FILE);
    assertTrue(file.exists());

    // Load peers
    Map<KademliaId, Peer> loadedPeers = Saver.loadPeers();

    // Verify loaded peers
    assertNotNull(loadedPeers);
    assertEquals(2, loadedPeers.size());

    Peer loadedPeer1 = loadedPeers.get(peer1.getKademliaId());
    assertNotNull(loadedPeer1);
    assertEquals(peer1.getIp(), loadedPeer1.getIp());
    assertEquals(peer1.getPort(), loadedPeer1.getPort());

    Peer loadedPeer2 = loadedPeers.get(peer2.getKademliaId());
    assertNotNull(loadedPeer2);
    assertEquals(peer2.getIp(), loadedPeer2.getIp());
    assertEquals(peer2.getPort(), loadedPeer2.getPort());
  }

  @Test
  public void testSavePeersSkipsUndialablePeers() {
    // T86: an inbound-only peer (a light client announces port 0 in the handshake) cannot be
    // dialled by anyone. Persisting it is what let hundreds of such entries survive restarts and
    // be re-gossiped afterwards - the affected node's peers.dat held 82 loopback entries alone.
    ArrayList<Peer> peers = new ArrayList<>();

    Peer inboundOnly = new Peer("84.147.60.253", 0);
    inboundOnly.setNodeId(new NodeId());
    peers.add(inboundOnly);

    Peer dialable = new Peer("46.224.156.238", 59558);
    dialable.setNodeId(new NodeId());
    peers.add(dialable);

    Saver.savePeers(peers);

    Map<KademliaId, Peer> loadedPeers = Saver.loadPeers();

    assertEquals(1, loadedPeers.size());
    assertNotNull(loadedPeers.get(dialable.getKademliaId()));
  }

  @Test
  public void testSavePeersSkipsPeerWithoutVerifyKey() {
    // Simulates the first-boot race: a bootstrap peer's KademliaId is known (e.g. from the DHT)
    // but its handshake has not completed yet, so its NodeId has no verify key. Saving must not
    // throw and must still persist the other, fully-identified peers.
    ArrayList<Peer> peers = new ArrayList<>();

    Peer keyedPeer = new Peer("127.0.0.1", 1234);
    keyedPeer.setNodeId(new NodeId());
    peers.add(keyedPeer);

    Peer bootstrapPeer = new Peer("10.0.0.1", 4321);
    bootstrapPeer.setNodeId(new NodeId(new KademliaId()));
    peers.add(bootstrapPeer);

    Saver.savePeers(peers);

    File file = new File(TEST_FILE);
    assertTrue(file.exists());

    Map<KademliaId, Peer> loadedPeers = Saver.loadPeers();

    assertNotNull(loadedPeers);
    assertEquals(1, loadedPeers.size());

    Peer loadedKeyedPeer = loadedPeers.get(keyedPeer.getKademliaId());
    assertNotNull(loadedKeyedPeer);
    assertEquals(keyedPeer.getIp(), loadedKeyedPeer.getIp());
    assertEquals(keyedPeer.getPort(), loadedKeyedPeer.getPort());

    assertTrue(
        loadedPeers.values().stream().noneMatch(p -> p.getIp().equals(bootstrapPeer.getIp())));
  }

  /**
   * savePeers used to be handed PeerList#getPeerArrayList() - the live list - and iterated it
   * without the peer list read lock, while network threads add and remove peers (redpandaj#260,
   * REDPANDAJ-2DZ). Asserted via the lock itself rather than a racing stress test: a savePeers that
   * takes the read lock cannot make progress while the write lock is held. Deterministic and
   * one-sided, see {@link ConcurrencyTestSupport}.
   */
  @Test(timeout = 60_000)
  public void savePeersBlocksWhileThePeerListWriteLockIsHeld() throws Exception {
    PeerList peerList = ServerContext.buildDefaultServerContext().getPeerList();
    Peer peer = new Peer("127.0.0.1", 1234);
    peer.setNodeId(new NodeId());
    peerList.add(peer);

    ConcurrencyTestSupport.assertBlockedWhileHeld(
        peerList.getReadWriteLock().writeLock(), () -> Saver.savePeers(peerList));

    Map<KademliaId, Peer> loadedPeers = Saver.loadPeers();
    assertEquals(1, loadedPeers.size());
    assertNotNull(loadedPeers.get(peer.getKademliaId()));
    assertFalse(new File(TEST_TMP_FILE).exists());
  }

  @Test
  public void testLoadMissingFile() {
    // Ensure file does not exist
    File file = new File(TEST_FILE);
    if (file.exists()) {
      file.delete();
    }

    // Load peers
    Map<KademliaId, Peer> loadedPeers = Saver.loadPeers();

    // Verify empty result (no exception)
    assertNotNull(loadedPeers);
    assertTrue(loadedPeers.isEmpty());
  }

  @Test
  public void testLoadCorruptedFile() throws IOException {
    // Determine path
    // Create directory if needed
    new File(TEST_SAVE_DIR).mkdirs();

    // Write garbage content
    Files.writeString(Path.of(TEST_FILE), "This is not a serialized object stream");

    // Load peers
    Map<KademliaId, Peer> loadedPeers = Saver.loadPeers();

    // Verify fallback to empty map
    assertNotNull(loadedPeers);
    assertTrue(loadedPeers.isEmpty());
  }
}
