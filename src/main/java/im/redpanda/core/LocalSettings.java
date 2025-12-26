package im.redpanda.core;

import im.redpanda.store.NodeEdge;
import lombok.extern.slf4j.Slf4j;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;

/**
 * @author Robin Braun
 */
@Slf4j
public class LocalSettings implements Serializable {


    @Serial
    private static final long serialVersionUID = 639L;

    private NodeId myIdentity;
    private String myIp;
    private long updateTimestamp;
    private byte[] updateSignature;

    private long updateAndroidTimestamp;
    private byte[] updateAndroidSignature;

    private DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph;

    private SystemUpTimeData systemUpTimeData;

    public LocalSettings() {
        myIdentity = new NodeId();
        myIp = "";
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

    public void save(int port) {
        try {

            File mkdirs = new File(Settings.SAVE_DIR);
            mkdirs.mkdir();

            File file = new File(Settings.SAVE_DIR + "/localSettings" + port + ".dat");

            file.createNewFile();
            try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
                    objectOutputStream.writeObject(this);
                }
            }

        } catch (IOException ex) {
            log.info("error saving local settings", ex);
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
