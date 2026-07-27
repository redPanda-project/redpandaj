package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import java.io.File;
import java.nio.file.Files;
import java.security.Security;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalSettingsPersistenceTest {

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  private int port;

  @Before
  public void setUp() {
    // Use a unique, unlikely port to avoid clobbering other tests/files
    port = 49123;
    // Cleanup pre-existing files if any
    deleteSettingsFiles();
  }

  @After
  public void tearDown() {
    deleteSettingsFiles();
  }

  private void deleteSettingsFiles() {
    // best effort
    settingsFile().delete();
    tmpSettingsFile().delete();
  }

  private File settingsFile() {
    return new File(Settings.SAVE_DIR + "/localSettings" + port + ".dat");
  }

  private File tmpSettingsFile() {
    return new File(Settings.SAVE_DIR + "/localSettings" + port + ".dat.tmp");
  }

  @Test
  public void saveAndLoadRoundtrip() {
    LocalSettings ls = new LocalSettings();
    ls.setUpdateTimestamp(123456789L);
    byte[] sig = new byte[] {1, 2, 3, 4};
    ls.setUpdateSignature(sig);
    ls.setUpdateAndroidTimestamp(222L);
    byte[] asig = new byte[] {9, 8, 7};
    ls.setUpdateAndroidSignature(asig);

    ls.save(port);

    LocalSettings loaded = LocalSettings.load(port);
    assertNotNull(loaded);
    assertThat(loaded.getUpdateTimestamp()).isEqualTo(123456789L);
    assertArrayEquals(sig, loaded.getUpdateSignature());
    assertThat(loaded.getUpdateAndroidTimestamp()).isEqualTo(222L);
    assertArrayEquals(asig, loaded.getUpdateAndroidSignature());
    assertNotNull(loaded.getMyIdentity());
    assertNotNull(loaded.getNodeGraph());
    assertNotNull(loaded.getSystemUpTimeData());
    assertThat(tmpSettingsFile()).doesNotExist();
  }

  /**
   * Regression test for REDPANDAJ-2E6: a save that blows up half way through serialization (there:
   * a ConcurrentModificationException on a collection another thread was mutating) must not destroy
   * the settings file that is already on disk — it holds the node identity.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void failedSaveKeepsPreviousFile() throws Exception {
    LocalSettings ls = new LocalSettings();
    ls.setUpdateTimestamp(4711L);
    ls.save(port);

    byte[] savedFile = Files.readAllBytes(settingsFile().toPath());

    // a vertex that is not serializable makes writeObject fail in the middle of the object graph,
    // just like the ConcurrentModificationException did
    ((DefaultDirectedWeightedGraph) ls.getNodeGraph()).addVertex(new Object());
    ls.setUpdateTimestamp(999L);

    ls.save(port);

    assertThat(LocalSettings.load(port).getUpdateTimestamp()).isEqualTo(4711L);
    // asserted as a boolean, an array comparison would dump both files into the failure message
    assertThat(Arrays.equals(savedFile, Files.readAllBytes(settingsFile().toPath())))
        .as("the settings file on disk must be byte identical to the last successful save")
        .isTrue();
    assertThat(tmpSettingsFile()).doesNotExist();
  }

  /**
   * SaveJobs and the update handling in InboundCommandProcessor both call save(), and both saves
   * write the same file (and the same temporary file). save() therefore has to hold the monitor of
   * the settings instance for the whole write.
   */
  @Test(timeout = 60_000)
  public void savesExcludeEachOther() throws Exception {
    LocalSettings ls = new LocalSettings();
    ls.setUpdateTimestamp(4711L);

    CountDownLatch saveStarted = new CountDownLatch(1);
    CountDownLatch saveFinished = new CountDownLatch(1);
    Thread saver =
        new Thread(
            () -> {
              saveStarted.countDown();
              ls.save(port);
              saveFinished.countDown();
            });

    synchronized (ls) {
      saver.start();
      assertThat(saveStarted.await(30, TimeUnit.SECONDS)).isTrue();
      // a save that does not take the monitor would be through in well under a second
      assertThat(saveFinished.await(1, TimeUnit.SECONDS))
          .as("save() must not write while another thread holds the settings monitor")
          .isFalse();
    }

    assertThat(saveFinished.await(30, TimeUnit.SECONDS)).isTrue();
    saver.join();
    assertThat(LocalSettings.load(port).getUpdateTimestamp()).isEqualTo(4711L);
    assertThat(tmpSettingsFile()).doesNotExist();
  }

  /**
   * The serialized node graph is the very object NodeStore mutates under its write lock (see the
   * REDPANDAJ-2DW comment in NodeStore#maintainNodes). Serializing it without the matching read
   * lock risks a ConcurrentModificationException; since #282 that no longer truncates the file, but
   * the save fails and the graph never reaches the disk.
   *
   * <p>Asserted via the lock itself instead of a racing stress test: a save that takes the read
   * lock cannot make progress while the write lock is held. That is deterministic and one-sided —
   * see {@link ConcurrencyTestSupport}.
   */
  @Test(timeout = 60_000)
  public void saveBlocksWhileTheNodeGraphWriteLockIsHeld() throws Exception {
    ServerContext serverContext = ServerContext.buildDefaultServerContext();
    LocalSettings localSettings = serverContext.getLocalSettings();
    localSettings.setUpdateTimestamp(4711L);

    // buildDefaultServerContext builds the NodeStore, which is where the graph and its lock are
    // handed over - so this also covers the wiring, not just LocalSettings itself.
    ConcurrencyTestSupport.assertBlockedWhileHeld(
        serverContext.getNodeStore().getReadWriteLock().writeLock(),
        () -> localSettings.save(port));

    assertThat(LocalSettings.load(port).getUpdateTimestamp()).isEqualTo(4711L);
    assertThat(tmpSettingsFile()).doesNotExist();
  }

  /** A LocalSettings that no NodeStore has adopted (e.g. Updater) still has to save. */
  @Test(timeout = 60_000)
  public void saveWorksWithoutANodeGraphLock() {
    LocalSettings localSettings = new LocalSettings();
    localSettings.setUpdateTimestamp(1234L);

    localSettings.save(port);

    assertThat(LocalSettings.load(port).getUpdateTimestamp()).isEqualTo(1234L);
    assertThat(tmpSettingsFile()).doesNotExist();
  }
}
