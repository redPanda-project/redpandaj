package im.redpanda.core;

import static org.junit.Assert.assertEquals;
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

  @Before
  @After
  public void cleanUp() throws IOException {
    File file = new File(TEST_FILE);
    if (file.exists()) {
      Files.delete(Path.of(TEST_FILE));
    }
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
