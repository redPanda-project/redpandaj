package im.redpanda.core;

import im.redpanda.store.NodeEdge;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import lombok.extern.slf4j.Slf4j;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;

/**
 * @author Robin Braun
 */
@Slf4j
public class LocalSettings implements Serializable {

  @Serial private static final long serialVersionUID = 639L;

  // NOTE (v22 removal, sdd02 phase 2): settings files written before 2026-07 additionally
  // contain a serialized im.redpanda.crypt.legacy.LegacyNodeId in the removed field
  // `legacyIdentity`. Java deserialization reads and discards such values via the tombstone
  // stub of that class — see LegacyNodeId and LocalSettingsLegacyFixtureTest.
  private NodeId myIdentity;

  private long updateTimestamp;
  private byte[] updateSignature;

  private long updateAndroidTimestamp;
  private byte[] updateAndroidSignature;

  private DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph;

  private SystemUpTimeData systemUpTimeData;

  public LocalSettings() {
    myIdentity = new NodeId();
    updateTimestamp = -1;
    nodeGraph = new DefaultDirectedWeightedGraph<>(NodeEdge.class);
    systemUpTimeData = new SystemUpTimeData();
  }

  public void setUpdateSignature(byte[] updateSignature) {
    this.updateSignature = updateSignature;
  }

  public byte[] getUpdateSignature() {
    return updateSignature;
  }

  public byte[] getUpdateAndroidSignature() {
    return updateAndroidSignature;
  }

  public void setUpdateAndroidSignature(byte[] updateAndroidSignature) {
    this.updateAndroidSignature = updateAndroidSignature;
  }

  /**
   * Writes the settings to disk atomically: the object graph is serialized into a temporary file
   * which only replaces the live file once it is complete. Serializing straight into the live file
   * truncates it first, so any failure half way through (a {@link
   * java.util.ConcurrentModificationException} from a collection mutated by another thread —
   * REDPANDAJ-2E6 —, a full disk, ...) left behind a truncated file that {@link #load(int)} cannot
   * read, and the node silently generated a new identity on the next start.
   *
   * <p>Synchronized because both the {@code SaveJobs} job and the update handling in {@code
   * InboundCommandProcessor} call this, and two saves running at once would write the same file.
   */
  public synchronized void save(int port) {
    File mkdirs = new File(Settings.SAVE_DIR);
    mkdirs.mkdir();

    File file = new File(Settings.SAVE_DIR + "/localSettings" + port + ".dat");
    File tmpFile = new File(Settings.SAVE_DIR + "/localSettings" + port + ".dat.tmp");

    try {
      try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
          ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
        objectOutputStream.writeObject(this);
        objectOutputStream.flush();
        // force the bytes to disk before the rename, otherwise a crash right after the rename
        // could leave an empty file where a complete old one used to be. Both streams are still
        // open here, try-with-resources closes them at the end of the block.
        fileOutputStream.getFD().sync();
      }

      Files.move(
          tmpFile.toPath(),
          file.toPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);

    } catch (IOException ex) {
      log.info("error saving local settings", ex);
    } finally {
      // on success the temporary file has been renamed away, on failure it is incomplete
      if (tmpFile.exists() && !tmpFile.delete()) {
        log.info("could not delete temporary settings file {}", tmpFile);
      }
    }
  }

  public static LocalSettings load(int port) {
    try {
      File file = new File(Settings.SAVE_DIR + "/localSettings" + port + ".dat");

      try (FileInputStream fileInputStream = new FileInputStream(file)) {
        try (ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
          return (LocalSettings) objectInputStream.readObject();
        }
      }

    } catch (ClassNotFoundException | ClassCastException | IOException ex) {
      log.info("error loading local settings", ex);
    }

    log.info("could not load localSettings.dat, generating new LocalSettings");

    LocalSettings localSettings = new LocalSettings();
    localSettings.save(port);
    return localSettings;
  }

  public long getUpdateTimestamp() {
    return updateTimestamp;
  }

  public void setUpdateTimestamp(long updateTimestamp) {
    this.updateTimestamp = updateTimestamp;
  }

  public long getUpdateAndroidTimestamp() {
    return updateAndroidTimestamp;
  }

  public void setUpdateAndroidTimestamp(long updateAndroidTimestamp) {
    this.updateAndroidTimestamp = updateAndroidTimestamp;
  }

  public NodeId getMyIdentity() {
    return myIdentity;
  }

  public DefaultDirectedWeightedGraph<Node, NodeEdge> getNodeGraph() {
    return nodeGraph;
  }

  public SystemUpTimeData getSystemUpTimeData() {
    if (systemUpTimeData == null) {
      systemUpTimeData = new SystemUpTimeData();
    }
    return systemUpTimeData;
  }
}
