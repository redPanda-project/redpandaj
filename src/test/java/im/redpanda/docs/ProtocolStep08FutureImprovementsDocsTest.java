package im.redpanda.docs;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ProtocolStep08FutureImprovementsDocsTest {

  @Test
  void documentationExistsAndStatesKeypoints() throws Exception {
    Path doc = Path.of("docs/protocol/08-future-improvements.md");
    String text = Files.readString(doc);
    assertTrue(text.contains("Title: Future Improvements"));
    assertTrue(text.contains("non-blocking"));
    assertTrue(text.contains("ChaCha20-Poly1305"));
  }
}
