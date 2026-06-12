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

  /** Lazy fallback for tests that build a ServerContext without LocalSettings. */
  @Deprecated private im.redpanda.crypt.legacy.LegacyNodeId fallbackLegacyNodeId;

  /**
   * The brainpool identity served to protocol-v22 light clients (MS03 transition phase only).
   *
   * @deprecated removed together with v22 support
   */
  @Deprecated
  public im.redpanda.crypt.legacy.LegacyNodeId getLegacyNodeId() {
    if (localSettings != null) {
      return localSettings.getLegacyIdentity();
    }
    synchronized (this) {
      if (fallbackLegacyNodeId == null) {
        fallbackLegacyNodeId = im.redpanda.crypt.legacy.LegacyNodeId.generate();
      }
      return fallbackLegacyNodeId;
    }
  }

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
