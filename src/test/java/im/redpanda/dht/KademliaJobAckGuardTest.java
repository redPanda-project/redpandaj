package im.redpanda.dht;

import static org.assertj.core.api.Assertions.assertThatCode;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.NodeId;
import im.redpanda.transport.Peer;
import java.security.Security;
import org.junit.jupiter.api.Test;

/**
 * Both Kademlia jobs keep their peers in a {@code ConcurrentSkipListMap} sorted by {@link
 * PeerComparator}, which dereferences {@code Peer.getKademliaId()}. Their {@code init()} only ever
 * puts peers that have a NodeId into that map — but {@code ack(...)} used to put whatever the wire
 * handed it, and {@code Job.start()} registers a job in the {@code JobRegistry} <em>before</em>
 * {@code init()} runs, so the map can still be null.
 *
 * <p>Both are reachable from the network: JOB_ACK (130) and KADEMLIA_GET_ANSWER (122) carry a job
 * id, and the dispatcher looks the job up and acks it. Either hole took the peer's command loop
 * down with an NPE.
 */
class KademliaJobAckGuardTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  private static KadContent signedContent() {
    NodeId author = NodeId.generateWithSimpleKey();
    KadContent content =
        new KadContent(System.currentTimeMillis(), author.exportPublic(), "content".getBytes());
    content.signWith(author);
    return content;
  }

  @Test
  void insertJob_ackBeforeInit_doesNotThrow() {
    ServerContext ctx = ServerContext.buildDefaultServerContext();
    KademliaInsertJob job = new KademliaInsertJob(ctx, signedContent());

    Peer peer = new Peer("127.0.0.1", 15401, new NodeId());
    peer.setConnected(true);

    assertThatCode(() -> job.ack(peer)).doesNotThrowAnyException();
  }

  @Test
  void insertJob_ackFromPeerWithoutNodeId_doesNotThrow() {
    ServerContext ctx = ServerContext.buildDefaultServerContext();
    KademliaInsertJob job = new KademliaInsertJob(ctx, signedContent());
    job.init();

    Peer lightClient = new Peer("127.0.0.1", 15402);
    lightClient.setConnected(true);

    assertThatCode(() -> job.ack(lightClient)).doesNotThrowAnyException();
  }

  @Test
  void searchJob_ackBeforeInit_doesNotThrow() {
    ServerContext ctx = ServerContext.buildDefaultServerContext();
    KademliaSearchJob job = new KademliaSearchJob(ctx, new NodeId().getKademliaId());

    Peer peer = new Peer("127.0.0.1", 15403, new NodeId());
    peer.setConnected(true);

    assertThatCode(() -> job.ack(signedContent(), peer)).doesNotThrowAnyException();
  }

  @Test
  void searchJob_ackFromPeerWithoutNodeId_doesNotThrow() {
    ServerContext ctx = ServerContext.buildDefaultServerContext();
    KademliaSearchJob job = new KademliaSearchJob(ctx, new NodeId().getKademliaId());
    job.init();

    Peer lightClient = new Peer("127.0.0.1", 15404);
    lightClient.setConnected(true);

    assertThatCode(() -> job.ack(signedContent(), lightClient)).doesNotThrowAnyException();
  }
}
