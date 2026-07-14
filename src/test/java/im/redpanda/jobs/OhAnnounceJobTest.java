package im.redpanda.jobs;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.ServerContext;
import im.redpanda.jobs.OhAnnounceJob.SingleAnnounceJob;
import java.security.SecureRandom;
import org.junit.Before;
import org.junit.Test;

/**
 * First-delivery-latency: a freshly registered OH must announce promptly (small jitter) so the
 * first deposits are not lost, while the periodic re-announce keeps the full anti-profiling
 * stagger. Verifies the two announce paths are scheduled with the correct stagger bound.
 */
public class OhAnnounceJobTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  private ServerContext serverContext;
  private byte[] ohId;

  @Before
  public void setUp() {
    serverContext = ServerContext.buildDefaultServerContext();
    ohId = new byte[20];
    RANDOM.nextBytes(ohId);
  }

  @Test
  public void newRegistrationStagger_isShortAndStrictlyBelowFullStagger() {
    assertThat(OhAnnounceJob.NEW_REGISTRATION_STAGGER_MS).isEqualTo(2_000L);
    assertThat(OhAnnounceJob.ANNOUNCE_STAGGER_MS).isEqualTo(30_000L);
    assertThat(OhAnnounceJob.NEW_REGISTRATION_STAGGER_MS)
        .as("a fresh registration must announce faster than the periodic re-announce")
        .isLessThan(OhAnnounceJob.ANNOUNCE_STAGGER_MS);
  }

  @Test
  public void newRegistration_isScheduledWithinTheShortStagger() {
    // sample many times: a new registration is never scheduled beyond the short stagger bound
    for (int i = 0; i < 1_000; i++) {
      SingleAnnounceJob job =
          new SingleAnnounceJob(serverContext, ohId, OhAnnounceJob.NEW_REGISTRATION_STAGGER_MS);
      assertThat(job.getMaxStaggerMs()).isEqualTo(OhAnnounceJob.NEW_REGISTRATION_STAGGER_MS);
      assertThat(job.getDelayMs()).isBetween(0L, OhAnnounceJob.NEW_REGISTRATION_STAGGER_MS);
    }
  }

  @Test
  public void periodicReAnnounce_keepsTheFullStagger() {
    // the periodic path is scheduled with the full anti-profiling stagger bound
    for (int i = 0; i < 1_000; i++) {
      SingleAnnounceJob job =
          new SingleAnnounceJob(serverContext, ohId, OhAnnounceJob.ANNOUNCE_STAGGER_MS);
      assertThat(job.getMaxStaggerMs()).isEqualTo(OhAnnounceJob.ANNOUNCE_STAGGER_MS);
      assertThat(job.getDelayMs()).isBetween(0L, OhAnnounceJob.ANNOUNCE_STAGGER_MS);
    }
  }
}
