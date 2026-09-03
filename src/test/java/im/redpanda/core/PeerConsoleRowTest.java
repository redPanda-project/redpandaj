package im.redpanda.core;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.identity.NodeId;
import org.junit.jupiter.api.Test;

/**
 * Pins {@link Peer#consoleStatusRow()}, which T118 extracted out of {@code ListenConsole} when the
 * console moved into the ops context: the row is built from eight package-private {@code Peer}
 * fields, so keeping the formatting in the console would have meant making them public again.
 */
class PeerConsoleRowTest {

  @Test
  void rowUsesTheColumnWidthsAndOrderOfTheConsoleTable() {
    Peer peer = new Peer("10.1.2.3", 59558);

    String row = peer.consoleStatusRow();

    assertThat(row).endsWith("\n");
    String[] columns = row.trim().split("\\s+");
    assertThat(columns).hasSize(10);
    assertThat(columns[0]).isEqualTo("[10.1.2.3]:59558");
    assertThat(columns[1]).isEqualTo("-"); // no NodeId yet
    assertThat(columns[2]).isEqualTo("-"); // no pong yet
    assertThat(columns[3]).isEqualTo("false/false"); // connected / authed+crypted
    assertThat(columns[4]).isEqualTo("0"); // retries
    assertThat(columns[6]).isEqualTo("-"); // rating placeholder
    assertThat(columns[7]).isEqualTo("0"); // sendBytes
    assertThat(columns[8]).isEqualTo("0"); // receivedBytes
    assertThat(columns[9]).isEqualTo("0"); // removedSendMessages
  }

  @Test
  void rowShowsTheFirstTenCharactersOfTheKademliaId() {
    Peer peer = new Peer("127.0.0.1", 1234);
    NodeId nodeId = new NodeId();
    peer.setNodeId(nodeId);

    String[] columns = peer.consoleStatusRow().trim().split("\\s+");

    assertThat(columns[1]).isEqualTo(nodeId.getKademliaId().toString().substring(0, 10));
  }
}
