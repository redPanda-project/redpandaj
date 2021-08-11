package im.redpanda.core;

import im.redpanda.kademlia.KadStoreManager;
import im.redpanda.store.NodeStore;

public class ServerContext {

    private int port;
    private LocalSettings localSettings;
    private final KadStoreManager kadStoreManager = new KadStoreManager(this);
    private PeerList peerList = new PeerList();
    private NodeStore nodeStore;
    private NodeId nodeId;
    private KademliaId nonce;


    public static ServerContext buildDefaultServerContext() {
        ServerContext serverContext = new ServerContext();
        serverContext.setPort(-1);
        serverContext.setLocalSettings(new LocalSettings());
        serverContext.setNodeId(serverContext.getLocalSettings().getMyIdentity());
        serverContext.setNonce(serverContext.getLocalSettings().getMyIdentity().getKademliaId());
        serverContext.setNodeStore(NodeStore.buildWithMemoryCacheOnly(serverContext));
        return serverContext;
    }

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

    public NodeStore getNodeStore() {
        return nodeStore;
    }

    public void setNodeStore(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public void setNodeId(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    public KademliaId getNonce() {
        return nonce;
    }

    public void setNonce(KademliaId nonce) {
        this.nonce = nonce;
    }

    public KadStoreManager getKadStoreManager() {
        return kadStoreManager;
    }
}
