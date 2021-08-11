package im.redpanda.core;

public class ServerContext {

    private int port;
    private LocalSettings localSettings;
    private PeerList peerList;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public LocalSettings getLocalSettings() {
        return localSettings;
    }

    public void setLocalSettings(LocalSettings localSettings) {
        this.localSettings = localSettings;
    }

    public PeerList getPeerList() {
        return peerList;
    }

    public void setPeerList(PeerList peerList) {
        this.peerList = peerList;
    }
}
