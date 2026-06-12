package im.redpanda.docs;

import static org.junit.Assert.*;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

public class ProtocolStep03ExceptionHandlingDocsTest {

  @Test
  public void documentationExistsAndStatesKeypoints() throws Exception {
    Path doc = Path.of("docs/protocol/03-exception-handling.md");
    String text = Files.readString(doc);
    assertTrue(text.contains("Title: Exception Handling Adjustments in Crypto"));
    assertTrue(text.contains("Removed ShortBufferException"));
    assertTrue(text.contains("AEADBadTagException"));
    assertTrue(text.contains("PeerProtocolException"));
  }
}
