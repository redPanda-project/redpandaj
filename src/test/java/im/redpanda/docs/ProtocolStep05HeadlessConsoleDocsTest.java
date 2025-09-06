package im.redpanda.docs;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertTrue;

public class ProtocolStep05HeadlessConsoleDocsTest {

    @Test
    public void documentationExistsAndStatesKeypoints() throws Exception {
        Path doc = Path.of("docs/protocol/05-headless-console.md");
        String text = Files.readString(doc);
        assertTrue(text.contains("Title: Headless Console Behavior in Tests"));
        assertTrue(text.contains("NullPointerException"));
        assertTrue(text.contains("allowlist"));
    }
}

