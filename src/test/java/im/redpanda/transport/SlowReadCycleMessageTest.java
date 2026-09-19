package im.redpanda.transport;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * TD226 / Sentry REDPANDAJ-2DQ ("read cycle took over 5 seconds", 2029 events since 2026-07-12).
 * The watchdog reported one lumped number for socket read + decrypt + dispatch including lock wait,
 * so no event could ever say which phase burned the time — the triage could not decide between a
 * slow aarch64 decrypt, a lock the reader waits on and a real stall. The line has to carry the
 * phases separately, and the lock wait must be attributable rather than folded into "dispatch"
 * invisibly.
 */
class SlowReadCycleMessageTest {

  @Test
  void theWatchdogLineCarriesEveryPhaseSeparately() {
    String message =
        ConnectionReaderThread.slowReadCycleMessage(5990L, 3L, 12L, 41L, 5930L, 161, "peer-42");

    System.out.println(message);

    assertThat(message)
        .contains("5990 ms total")
        .contains("socket read 3 ms")
        .contains("readBuffer lock wait 12 ms")
        .contains("decrypt 41 ms")
        .contains("dispatch 5930 ms")
        .contains("last parsed command byte: 161")
        .contains("peer: peer-42");
  }
}
