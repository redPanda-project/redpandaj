package im.redpanda.docs;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ProtocolStep07OperationalNotesDocsTest {

  @Test
  void documentationExistsAndStatesKeypoints() throws Exception {
    Path doc = Path.of("docs/protocol/07-operational-notes.md");
    String text = Files.readString(doc);
    assertTrue(text.contains("Title: Operational Notes"));
    assertTrue(text.contains("ciphertext || tag"));
    assertTrue(text.contains("Log hygiene"));
  }
}
