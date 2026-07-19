package im.redpanda.core;

import im.redpanda.kademlia.KadStoreManager;
import im.redpanda.outbound.OutboundHandleStore;
import im.redpanda.outbound.OutboundMailboxStore;
import im.redpanda.outbound.OutboundService;
import im.redpanda.store.NodeStore;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class ServerContext {

  private int port;
  private LocalSettings localSettings;
  private final KadStoreManager kadStoreManager = new KadStoreManager(this);
  private PeerList peerList = new PeerList(this);
  private NodeStore nodeStore;
  private Node node;
  private NodeId nodeId;
  private KademliaId nonce;
  private ConnectionHandler connectionHandler;

  private OutboundService outboundService;
  private OutboundHandleStore outboundHandleStore;
  private OutboundMailboxStore outboundMailboxStore;

  public static ServerContext buildDefaultServerContext() {
    ServerContext serverContext = new ServerContext();
    serverContext.setPort(-1);
    serverContext.setLocalSettings(new LocalSettings());
    serverContext.setNodeId(serverContext.getLocalSettings().getMyIdentity());
    serverContext.setNonce(serverContext.getLocalSettings().getMyIdentity().getKademliaId());
    serverContext.setNodeStore(NodeStore.buildWithMemoryCacheOnly(serverContext));
    return serverContext;
  }
}
