package im.redpanda.core;

import im.redpanda.dht.KadStoreManager;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import im.redpanda.mailbox.OutboundService;
import im.redpanda.mailbox.OutboundStore;
import im.redpanda.ops.JobRegistry;
import im.redpanda.routing.graph.Node;
import im.redpanda.routing.graph.NodeStore;
import im.redpanda.transport.ConnectionHandler;
import im.redpanda.transport.PeerList;
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

  /**
   * The jobs running for this node. Instance state since T118: the map used to be a static on
   * {@code Job}, shared by every {@code ServerContext} in the JVM.
   */
  private final JobRegistry jobRegistry = new JobRegistry();

  private PeerList peerList = new PeerList();
  private NodeStore nodeStore;
  private Node node;
  private NodeId nodeId;

  /**
   * This node's own Kademlia identity: the 20-byte id of {@link #nodeId}, i.e. the DHT address
   * other nodes route to and the value this node announces in the handshake.
   *
   * <p>This used to be called {@code nonce}, which was a misnomer -- it is a stable identity, not a
   * per-message random value. The rename (T113) is code-level only; nothing on the wire or in
   * persisted node state carries the name (the handshake sends the raw 20 bytes, and {@code
   * LocalSettings} persists the identity as {@code NodeId}, not as this value).
   *
   * <p><b>Derived, not stored (T118 / TD146).</b> It used to be a second field that {@code
   * setNodeId} did not keep in sync -- an invariant held by convention, not by code. All three
   * writers (this class, {@code App#main}, {@code TestNodeLauncher}) set it from {@code
   * getNodeId().getKademliaId()} in the statement right after {@code setNodeId}, so deriving it
   * produces the same value at every read while making a later {@code setNodeId} take effect
   * instead of silently drifting. {@code NodeId.getKademliaId()} caches, so this stays a field read
   * after the first call. A context without a {@code nodeId} yields {@code null}, exactly as the
   * uninitialised field did.
   */
  public KademliaId getOwnNodeId() {
    return nodeId == null ? null : nodeId.getKademliaId();
  }

  private ConnectionHandler connectionHandler;

  private OutboundService outboundService;
  private OutboundStore outboundStore;

  public static ServerContext buildDefaultServerContext() {
    ServerContext serverContext = new ServerContext();
    serverContext.setPort(-1);
    serverContext.setLocalSettings(new LocalSettings());
    serverContext.setNodeId(serverContext.getLocalSettings().getMyIdentity());
    serverContext.setNodeStore(NodeStore.buildWithMemoryCacheOnly(serverContext));
    return serverContext;
  }
}
