package im.redpanda.docs;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

public class ProtocolStep07OperationalNotesDocsTest {

  @Test
  public void documentationExistsAndStatesKeypoints() throws Exception {
    Path doc = Path.of("docs/protocol/07-operational-notes.md");
    String text = Files.readString(doc);
    assertTrue(text.contains("Title: Operational Notes"));
    assertTrue(text.contains("ciphertext || tag"));
    assertTrue(text.contains("Log hygiene"));
  }
}
