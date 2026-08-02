package im.redpanda.jobs;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import im.redpanda.core.KademliaId;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KademliaSearchJobHousekeeperTest {

  private final Map<KademliaId, Long> blacklistBackup = new HashMap<>();

  @BeforeEach
  void backupBlacklist() {
    KademliaSearchJob.getKademliaIdSearchBlacklistLock().lock();
    try {
      blacklistBackup.putAll(KademliaSearchJob.getKademliaIdSearchBlacklist());
      KademliaSearchJob.getKademliaIdSearchBlacklist().clear();
    } finally {
      KademliaSearchJob.getKademliaIdSearchBlacklistLock().unlock();
    }
  }

  @AfterEach
  void restoreBlacklist() {
    KademliaSearchJob.getKademliaIdSearchBlacklistLock().lock();
    try {
      KademliaSearchJob.getKademliaIdSearchBlacklist().clear();
      KademliaSearchJob.getKademliaIdSearchBlacklist().putAll(blacklistBackup);
    } finally {
      KademliaSearchJob.getKademliaIdSearchBlacklistLock().unlock();
    }
  }

  @Test
  void work_evictsExpiredEntriesAndKeepsLiveOnes() {
    KademliaId expired = new KademliaId();
    KademliaId live = new KademliaId();

    long now = System.currentTimeMillis();
    KademliaSearchJob.getKademliaIdSearchBlacklistLock().lock();
    try {
      KademliaSearchJob.getKademliaIdSearchBlacklist().put(expired, now - 1000L);
      // same lifetime the production code hands out
      KademliaSearchJob.getKademliaIdSearchBlacklist()
          .put(live, now + KademliaSearchJob.BLACKLIST_KEY_FOR);
    } finally {
      KademliaSearchJob.getKademliaIdSearchBlacklistLock().unlock();
    }

    new KademliaSearchJobHousekeeper(null).work();

    Map<KademliaId, Long> blacklist = snapshotBlacklist();
    assertThat(blacklist).doesNotContainKey(expired);
    assertThat(blacklist).containsKey(live);
  }

  /** The blacklist is a plain HashMap guarded by its own lock — never read it unsynchronized. */
  private static Map<KademliaId, Long> snapshotBlacklist() {
    KademliaSearchJob.getKademliaIdSearchBlacklistLock().lock();
    try {
      return new HashMap<>(KademliaSearchJob.getKademliaIdSearchBlacklist());
    } finally {
      KademliaSearchJob.getKademliaIdSearchBlacklistLock().unlock();
    }
  }

  /**
   * The blacklist is fed from inbound, peer-controlled search requests, so the eviction interval is
   * the only bound on its size. It must stay in the same order of magnitude as the entry lifetime.
   */
  @Test
  void runIntervalIsBoundedByTheBlacklistLifetime() {
    assertThat(KademliaSearchJobHousekeeper.RUN_INTERVAL)
        .isLessThanOrEqualTo(4L * KademliaSearchJob.BLACKLIST_KEY_FOR);
  }

  /**
   * Regression for the bug hunt finding "housekeeper is never started": the class existed and was
   * correct, but nothing instantiated it, so the blacklist grew for the whole process lifetime.
   */
  @Test
  void housekeeperIsWiredUpInApp() {
    JavaClasses importedClasses = new ClassFileImporter().importPackages("im.redpanda");

    ArchRule rule =
        classes()
            .that()
            .haveFullyQualifiedName("im.redpanda.App")
            .should()
            .dependOnClassesThat()
            .haveFullyQualifiedName("im.redpanda.jobs.KademliaSearchJobHousekeeper")
            .because(
                "the KademliaSearchJob blacklist is only bounded if the housekeeper is started");

    rule.check(importedClasses);
  }
}
