package im.redpanda.docs;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertTrue;

public class ProtocolStep01NodeIdDocsTest {

    @Test
    public void documentationExistsAndStatesKeypoints() throws Exception {
        Path doc = Path.of("docs/protocol/01-nodeid-keypair.md");
        String text = Files.readString(doc);
        assertTrue(text.contains("Title: NodeId.setKeyPair Protocol and Guarantees"));
        assertTrue(text.contains("Reject null"));
        assertTrue(text.contains("Reject duplicate set"));
        assertTrue(text.contains("Require known KademliaId"));
        assertTrue(text.contains("Validate match"));
    }
}

