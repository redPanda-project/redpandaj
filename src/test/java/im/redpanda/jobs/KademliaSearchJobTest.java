package im.redpanda.jobs;

import im.redpanda.core.KademliaId;
import im.redpanda.core.ServerContext;
import org.junit.Test;

public class KademliaSearchJobTest {

  private static final ServerContext serverContext = ServerContext.buildDefaultServerContext();

  /**
   * Regression for REDPANDAJ-2E3: a second search for the same id started within the blacklist
   * window ({@code init()} calls {@code fail(); done(); return;} before {@code peers} is ever
   * assigned) must not NPE if {@code work()} still runs for that job (e.g. a concurrently
   * already-dispatched run() reaching work() in the same window the first job's init() is
   * blacklisting the id).
   */
  @Test
  public void work_afterBlacklistedInitDoesNotThrow() {
    KademliaId id = new KademliaId();

    KademliaSearchJob first = new KademliaSearchJob(serverContext, id);
    first.init();

    // Second search for the same id, still inside the blacklist window: takes the
    // early-return branch (fail(); done(); return;) without ever assigning `peers`.
    KademliaSearchJob blacklisted = new KademliaSearchJob(serverContext, id);
    blacklisted.init();

    // Must not throw NullPointerException on peers.keySet().
    blacklisted.work();
  }
}
