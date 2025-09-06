package im.redpanda.docs;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertTrue;

public class ProtocolStep08FutureImprovementsDocsTest {

    @Test
    public void documentationExistsAndStatesKeypoints() throws Exception {
        Path doc = Path.of("docs/protocol/08-future-improvements.md");
        String text = Files.readString(doc);
        assertTrue(text.contains("Title: Future Improvements"));
        assertTrue(text.contains("non-blocking"));
        assertTrue(text.contains("ChaCha20-Poly1305"));
    }
}

