package im.redpanda.docs;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertTrue;

public class ProtocolStep04E2EDocsTest {

    @Test
    public void documentationExistsAndStatesKeypoints() throws Exception {
        Path doc = Path.of("docs/protocol/04-e2e-shutdown-and-storage.md");
        String text = Files.readString(doc);
        assertTrue(text.contains("Title: E2E Shutdown and Storage Isolation"));
        assertTrue(text.contains("Graceful shutdown"));
        assertTrue(text.contains("Isolated working dirs"));
    }
}

