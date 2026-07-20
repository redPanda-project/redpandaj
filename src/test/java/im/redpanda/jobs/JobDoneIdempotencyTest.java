package im.redpanda.jobs;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Regression for REDPANDAJ-2E2 / REDPANDAJ-2EA ("CODE 17dh6"). {@link Job#done()} used to check the
 * {@code done} flag outside {@code runningJobsLock} while the actual removal ran under it, so two
 * threads could both pass the guard and both deregister the job — the loser found it already gone
 * and threw. {@code done()} must now be atomic and idempotent: it deregisters the job exactly once
 * and any further call is a no-op.
 */
public class JobDoneIdempotencyTest {

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
  public void doneIsIdempotentAndDeregistersOnce() {
    Job job = noopJob();
    job.start();
    Integer jobId = job.getJobId();
    assertThat(Job.getRunningJob(jobId)).isSameAs(job);

    job.done();
    assertThat(Job.getRunningJob(jobId)).isNull();

    // A second done() must be a no-op, not throw "CODE 17dh6".
    job.done();
    assertThat(Job.getRunningJob(jobId)).isNull();
  }

  @Test
  public void concurrentDoneDeregistersExactlyOnceWithoutError() throws Exception {
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
    assertThat(Job.getRunningJob(jobId)).isNull();
  }
}
