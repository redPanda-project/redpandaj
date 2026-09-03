package im.redpanda.ops;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Regression for REDPANDAJ-2E2 / REDPANDAJ-2EA ("CODE 17dh6"). {@link Job#done()} used to check the
 * {@code done} flag outside {@code runningJobsLock} while the actual removal ran under it, so two
 * threads could both pass the guard and both deregister the job — the loser found it already gone
 * and threw. {@code done()} must now be atomic and idempotent: it deregisters the job exactly once
 * and any further call is a no-op.
 */
class JobDoneIdempotencyTest {

  private static final ServerContext serverContext = ServerContext.buildDefaultServerContext();

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  private static Job noopJob() {
    return new Job(serverContext) {
      @Override
      public void init() {}

      @Override
      public void work() {}
    };
  }

  @Test
  void doneIsIdempotentAndDeregistersOnce() {
    Job job = noopJob();
    job.start();
    Integer jobId = job.getJobId();
    assertThat(serverContext.getJobRegistry().get(jobId)).isSameAs(job);

    job.done();
    assertThat(serverContext.getJobRegistry().get(jobId)).isNull();

    // A second done() must be a no-op, not throw "CODE 17dh6".
    job.done();
    assertThat(serverContext.getJobRegistry().get(jobId)).isNull();
  }

  @Test
  void concurrentDoneDeregistersExactlyOnceWithoutError() throws Exception {
    Job job = noopJob();
    job.start();
    Integer jobId = job.getJobId();

    int threadCount = 8;
    CountDownLatch startGate = new CountDownLatch(1);
    List<Thread> workers = new ArrayList<>();
    AtomicReference<Throwable> escaped = new AtomicReference<>();

    for (int i = 0; i < threadCount; i++) {
      Thread t =
          new Thread(
              () -> {
                try {
                  startGate.await();
                  job.done();
                } catch (Throwable e) {
                  escaped.set(e);
                }
              });
      workers.add(t);
      t.start();
    }

    startGate.countDown();
    for (Thread t : workers) {
      t.join();
    }

    assertThat(escaped.get()).isNull();
    assertThat(serverContext.getJobRegistry().get(jobId)).isNull();
  }

  /**
   * {@code start()} must never replace a job that is already registered: the id used to be a bare
   * {@code rand.nextInt()} that was {@code put} into the map without checking, so a collision would
   * have re-routed the older job's ACK and let its {@code done()} deregister the newer job. The
   * registry draws the id under its lock now; starting many jobs must leave every one of them
   * retrievable under its own id.
   */
  @Test
  void startNeverOverwritesAnAlreadyRegisteredJob() {
    List<Job> jobs = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      Job job = noopJob();
      job.start();
      jobs.add(job);
    }

    for (Job job : jobs) {
      assertThat(serverContext.getJobRegistry().get(job.getJobId())).isSameAs(job);
    }
    assertThat(jobs.stream().map(Job::getJobId).distinct()).hasSize(jobs.size());

    jobs.forEach(Job::done);
  }
}
