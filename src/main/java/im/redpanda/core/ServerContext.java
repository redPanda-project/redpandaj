package im.redpanda.core;

import im.redpanda.dht.KadStoreManager;
import im.redpanda.mailbox.OutboundService;
import im.redpanda.mailbox.OutboundStore;
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
  private PeerList peerList = new PeerList();
  private NodeStore nodeStore;
  private Node node;
  private NodeId nodeId;

  /**
   * This node's own Kademlia identity: the 20-byte id of {@link #nodeId}, i.e. the DHT address
   * other nodes route to and the value this node announces in the handshake.
   *
   * <p>This field used to be called {@code nonce}, which was a misnomer -- it is a stable identity,
   * not a per-message random value. The rename is code-level only; nothing on the wire or in
   * persisted node state carries the name (the handshake sends the raw 20 bytes, and {@code
   * LocalSettings} persists the identity as {@code NodeId}, not as this field).
   *
   * <p><b>Redundant on purpose (for now):</b> this duplicates {@code nodeId.getKademliaId()}, and
   * the generated {@code setNodeId} does <i>not</i> keep it in sync. Both are written together
   * exactly once per node -- at startup, in {@link #buildDefaultServerContext()} and in {@code
   * App#main} -- and never again, so the two cannot drift today. Deriving it from {@code nodeId}
   * instead of storing it would be a behavioural change (every later {@code setNodeId} would start
   * to take effect) and belongs to the identity context of T118, not to this rename.
   */
  private KademliaId ownNodeId;

  private ConnectionHandler connectionHandler;

  private OutboundService outboundService;
  private OutboundStore outboundStore;

  public static ServerContext buildDefaultServerContext() {
    ServerContext serverContext = new ServerContext();
    serverContext.setPort(-1);
    serverContext.setLocalSettings(new LocalSettings());
    serverContext.setNodeId(serverContext.getLocalSettings().getMyIdentity());
    serverContext.setOwnNodeId(serverContext.getLocalSettings().getMyIdentity().getKademliaId());
    serverContext.setNodeStore(NodeStore.buildWithMemoryCacheOnly(serverContext));
    return serverContext;
  }
}
