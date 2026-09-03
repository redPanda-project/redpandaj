package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.dht.KadContent;
import im.redpanda.dht.KadStoreManager;
import im.redpanda.identity.NodeId;
import im.redpanda.ops.Job;
import java.security.Security;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Pins the context boundary T118 introduced: two {@link ServerContext}s in one JVM must not see
 * each other's state.
 *
 * <p>Before T118 the running-job map ({@code Job.runningJobs}) and the whole DHT custodian store
 * ({@code KadStoreManager.entries} plus its lock, sweep timestamp and byte counter) were {@code
 * static}, so a peer answer arriving at node A could resolve to a job of node B and both nodes
 * stored into the same map — even though {@code ServerContext} has owned a {@code KadStoreManager}
 * per node all along. Both are instance state now; these tests fail if either goes back to being
 * shared.
 */
class PerContextStateScopeTest {

  @BeforeAll
  static void addProvider() {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  private static Job noopJob(ServerContext serverContext) {
    return new Job(serverContext) {
      @Override
      public void init() {}

      @Override
      public void work() {}
    };
  }

  @Test
  void jobRegistriesAreSeparatePerContext() {
    ServerContext a = ServerContext.buildDefaultServerContext();
    ServerContext b = ServerContext.buildDefaultServerContext();
    assertThat(a.getJobRegistry()).isNotSameAs(b.getJobRegistry());

    int sharedJobId = 4711;
    Job jobOfA = noopJob(a);
    Job jobOfB = noopJob(b);
    a.getJobRegistry().register(sharedJobId, jobOfA);
    b.getJobRegistry().register(sharedJobId, jobOfB);

    // the same id resolves to this node's job, not to the other node's
    assertThat(a.getJobRegistry().get(sharedJobId)).isSameAs(jobOfA);
    assertThat(b.getJobRegistry().get(sharedJobId)).isSameAs(jobOfB);

    // an id only node A knows is invisible to node B
    Job onlyInA = noopJob(a);
    a.getJobRegistry().register(99, onlyInA);
    assertThat(b.getJobRegistry().get(99)).isNull();
  }

  @Test
  void startAndDoneOnlyTouchTheOwnRegistry() {
    ServerContext a = ServerContext.buildDefaultServerContext();
    ServerContext b = ServerContext.buildDefaultServerContext();

    Job jobOfA = noopJob(a);
    jobOfA.start();
    int jobId = jobOfA.getJobId();

    assertThat(a.getJobRegistry().get(jobId)).isSameAs(jobOfA);
    assertThat(b.getJobRegistry().get(jobId)).isNull();

    Job jobOfB = noopJob(b);
    b.getJobRegistry().register(jobId, jobOfB);

    jobOfA.done();

    assertThat(a.getJobRegistry().get(jobId)).isNull();
    assertThat(b.getJobRegistry().get(jobId)).isSameAs(jobOfB);
  }

  @Test
  void dhtStoresAreSeparatePerContext() {
    ServerContext a = ServerContext.buildDefaultServerContext();
    ServerContext b = ServerContext.buildDefaultServerContext();
    KadStoreManager storeOfA = a.getKadStoreManager();
    KadStoreManager storeOfB = b.getKadStoreManager();
    assertThat(storeOfA).isNotSameAs(storeOfB);

    KadContent content =
        new KadContent(System.currentTimeMillis(), new NodeId().exportPublic(), new byte[16]);
    assertThat(storeOfA.put(content)).isTrue();

    assertThat(storeOfA.get(content.getId())).isNotNull();
    assertThat(storeOfB.get(content.getId())).isNull();
  }
}
