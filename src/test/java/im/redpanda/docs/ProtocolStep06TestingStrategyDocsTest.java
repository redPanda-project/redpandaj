package im.redpanda.docs;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ProtocolStep06TestingStrategyDocsTest {

  @Test
  void documentationExistsAndStatesKeypoints() throws Exception {
    Path doc = Path.of("docs/protocol/06-testing-strategy.md");
    String text = Files.readString(doc);
    assertTrue(text.contains("Title: Testing Strategy Enhancements"));
    assertTrue(text.contains("Unit tests"));
    assertTrue(text.contains("Failsafe"));
  }
}
