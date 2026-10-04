package im.redpanda.ops;

import im.redpanda.transport.Peer;

/**
 * Deliberate offender for {@code BoundedContextArchitectureTest}: an {@code ops} class that reaches
 * into {@code transport}, which the TD173 rule must reject. Test source only, so it never reaches
 * the production class set the real rule checks.
 */
public final class OpsCycleFixture {

  private OpsCycleFixture() {}

  public static String describe(Peer peer) {
    return peer.toString();
  }
}
