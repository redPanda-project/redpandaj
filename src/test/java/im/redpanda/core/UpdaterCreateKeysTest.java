package im.redpanda.core;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The key-ceremony CLI entry point (T10/T13) must never overwrite an existing signing key file —
 * accidental key loss would permanently break the update channel. No real keys are ever generated
 * here: the guard fires before key generation.
 */
public class UpdaterCreateKeysTest {

  private static final Path KEY_FILE = Path.of("privateSigningKey.txt");
  private static final String DUMMY_CONTENT = "dummy-not-a-key";

  @Before
  public void setup() throws IOException {
    Files.writeString(KEY_FILE, DUMMY_CONTENT);
  }

  @After
  public void cleanup() throws IOException {
    Files.deleteIfExists(KEY_FILE);
  }

  @Test
  public void createNewKeys_refusesToOverwriteExistingKeyFile() throws IOException {
    Updater.createNewKeys();
    assertEquals(DUMMY_CONTENT, Files.readString(KEY_FILE));
  }

  @Test
  public void mainCreateKeys_onlyRunsCreateNewKeys_andKeepsExistingFile() throws IOException {
    // --create-keys must go through the same guard and must not fall through to the
    // update-inserting code path.
    Updater.main(new String[] {"--create-keys"});
    assertEquals(DUMMY_CONTENT, Files.readString(KEY_FILE));
  }
}
