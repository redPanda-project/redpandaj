package im.redpanda.docs;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ProtocolStep04E2EDocsTest {

  @Test
  void documentationExistsAndStatesKeypoints() throws Exception {
    Path doc = Path.of("docs/protocol/04-e2e-shutdown-and-storage.md");
    String text = Files.readString(doc);
    assertTrue(text.contains("Title: E2E Shutdown and Storage Isolation"));
    assertTrue(text.contains("Graceful shutdown"));
    assertTrue(text.contains("Isolated working dirs"));
  }
}
