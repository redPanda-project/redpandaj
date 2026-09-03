package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Pins the dispatch table of the T116 dispatcher.
 *
 * <p>Two invariants that used to be implicit and are now load-bearing:
 *
 * <ul>
 *   <li>the set of commands the processor answers at all — a handler silently lost while moving
 *       bodies into their domains would otherwise only show up as a peer disconnect ("protocol
 *       desync: unknown command") in production;
 *   <li>which of those are length-prefixed. Before T116 this list was maintained a second time by
 *       hand in {@code isPayloadCommand}; it is now derived from the registrations, and this test
 *       is the check that the derived set is still exactly the historical one.
 * </ul>
 */
class InboundCommandProcessorDispatchTableTest {

  @SuppressWarnings("unchecked")
  private static <T> T field(InboundCommandProcessor proc, String name) throws Exception {
    Field f = InboundCommandProcessor.class.getDeclaredField(name);
    f.setAccessible(true);
    return (T) f.get(proc);
  }

  private static InboundCommandProcessor newProcessor() {
    return new InboundCommandProcessor(ServerContext.buildDefaultServerContext());
  }

  @Test
  void everyWireCommandTheNodeAnswersIsRegistered() throws Exception {
    Map<Byte, ?> handlers = field(newProcessor(), "commandHandlers");

    assertThat(handlers.keySet())
        .containsExactlyInAnyOrder(
            Command.PING,
            Command.PONG,
            Command.REQUEST_PEERLIST,
            Command.SEND_PEERLIST,
            Command.UPDATE_REQUEST_TIMESTAMP,
            Command.UPDATE_ANSWER_TIMESTAMP,
            Command.UPDATE_REQUEST_CONTENT,
            Command.UPDATE_ANSWER_CONTENT,
            Command.ANDROID_UPDATE_REQUEST_TIMESTAMP,
            Command.ANDROID_UPDATE_ANSWER_TIMESTAMP,
            Command.ANDROID_UPDATE_REQUEST_CONTENT,
            Command.ANDROID_UPDATE_ANSWER_CONTENT,
            Command.JOB_ACK,
            Command.KADEMLIA_GET,
            Command.KADEMLIA_STORE,
            Command.KADEMLIA_GET_ANSWER,
            Command.FLASCHENPOST_PUT,
            Command.FLASCHENPOST_V2,
            Command.OUTBOUND_REGISTER_OH_REQ,
            Command.OUTBOUND_FETCH_REQ,
            Command.OUTBOUND_REVOKE_OH_REQ,
            Command.OUTBOUND_ACK_FETCH_REQ,
            Command.OUTBOUND_SUBSCRIBE_REQ);
  }

  /** Exactly the list {@code isPayloadCommand} spelled out by hand before T116. */
  @Test
  void framedCommandsAreExactlyTheHistoricalPayloadCommands() throws Exception {
    Set<Byte> framed = field(newProcessor(), "framedCommands");

    assertThat(framed)
        .containsExactlyInAnyOrder(
            Command.SEND_PEERLIST,
            Command.JOB_ACK,
            Command.KADEMLIA_GET,
            Command.KADEMLIA_STORE,
            Command.KADEMLIA_GET_ANSWER,
            Command.FLASCHENPOST_PUT,
            Command.FLASCHENPOST_V2,
            Command.OUTBOUND_REGISTER_OH_REQ,
            Command.OUTBOUND_FETCH_REQ,
            Command.OUTBOUND_REVOKE_OH_REQ,
            Command.OUTBOUND_ACK_FETCH_REQ,
            Command.OUTBOUND_SUBSCRIBE_REQ);
  }

  /** Every framed command must be dispatchable, and nothing may be framed without a handler. */
  @Test
  void framedCommandsAreASubsetOfTheDispatchTable() throws Exception {
    InboundCommandProcessor proc = newProcessor();
    Map<Byte, ?> handlers = field(proc, "commandHandlers");
    Set<Byte> framed = field(proc, "framedCommands");

    assertThat(handlers.keySet()).containsAll(framed);
  }
}
