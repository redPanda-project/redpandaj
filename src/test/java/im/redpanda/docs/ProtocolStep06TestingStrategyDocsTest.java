package im.redpanda.docs;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertTrue;

public class ProtocolStep06TestingStrategyDocsTest {

    @Test
    public void documentationExistsAndStatesKeypoints() throws Exception {
        Path doc = Path.of("docs/protocol/06-testing-strategy.md");
        String text = Files.readString(doc);
        assertTrue(text.contains("Title: Testing Strategy Enhancements"));
        assertTrue(text.contains("Unit tests"));
        assertTrue(text.contains("Failsafe"));
    }
}

