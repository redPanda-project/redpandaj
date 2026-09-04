package im.redpanda.updater;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import im.redpanda.identity.NodeId;
import im.redpanda.identity.crypt.Base58;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The key-ceremony CLI entry point (T10/T13). It must never overwrite an existing signing key file
 * — accidental key loss would permanently break the update channel — and the key it does create
 * must be usable and not world-readable.
 *
 * <p>The key file is redirected into a {@link TempDir} via {@code
 * redpanda.updater.signing.key.path} (T121). Before that, this test wrote its dummy key file into
 * the working directory, which the 8 Surefire forks share, and a test that lets the generator
 * actually run would have dropped a real private signing key into the checkout.
 */
class UpdaterCreateKeysTest {

  private static final String DUMMY_CONTENT = "dummy-not-a-key";

  @TempDir File tempDir;

  private Path keyFile;

  /** Whatever the property held before this test, so a value set from outside survives. */
  private String previousKeyPath;

  @BeforeEach
  void setup() {
    keyFile = new File(tempDir, "privateSigningKey.txt").toPath();
    previousKeyPath = System.getProperty(Updater.SIGNING_KEY_PATH_PROPERTY);
    System.setProperty(Updater.SIGNING_KEY_PATH_PROPERTY, keyFile.toAbsolutePath().toString());
  }

  @AfterEach
  void cleanup() {
    if (previousKeyPath == null) {
      System.clearProperty(Updater.SIGNING_KEY_PATH_PROPERTY);
    } else {
      System.setProperty(Updater.SIGNING_KEY_PATH_PROPERTY, previousKeyPath);
    }
  }

  @Test
  void createNewKeys_refusesToOverwriteExistingKeyFile() throws Exception {
    Files.writeString(keyFile, DUMMY_CONTENT);
    Updater.createNewKeys();
    assertEquals(DUMMY_CONTENT, Files.readString(keyFile));
  }

  @Test
  void mainCreateKeys_onlyRunsCreateNewKeys_andKeepsExistingFile() throws Exception {
    // --create-keys must go through the same guard and must not fall through to the
    // update-inserting code path.
    Files.writeString(keyFile, DUMMY_CONTENT);
    Updater.main(new String[] {"--create-keys"});
    assertEquals(DUMMY_CONTENT, Files.readString(keyFile));
  }

  /**
   * The other side of the guard: with no key file there, the ceremony produces one that {@code
   * insertNewUpdate()} can actually import — and that the rest of the machine cannot read.
   */
  @Test
  void createNewKeys_writesAnImportableOwnerOnlyKey() throws Exception {
    assertFalse(Files.exists(keyFile), "precondition: no key yet");

    Updater.createNewKeys();

    assertTrue(Files.exists(keyFile), "the ceremony must have written a key");
    NodeId imported = NodeId.importWithPrivate(Base58.decode(Files.readString(keyFile).trim()));
    assertEquals(NodeId.PUBLIC_KEYLEN, imported.exportPublic().length);
    // it is a private key, so it must be able to sign
    byte[] payload = "ceremony".getBytes();
    assertTrue(imported.verify(payload, imported.sign(payload)), "the key must be usable");

    try {
      Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(keyFile);
      assertEquals(
          Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE),
          permissions,
          "a private signing key must not be readable by anyone else");
    } catch (UnsupportedOperationException | IOException nonPosix) {
      // non-POSIX filesystem: nothing to assert, the key is still never printed anywhere
    }
  }

  /** A ceremony that just created a key must refuse to run again over it. */
  @Test
  void createNewKeys_isNotRepeatable() throws Exception {
    Updater.createNewKeys();
    String created = Files.readString(keyFile);

    Updater.createNewKeys();

    assertEquals(created, Files.readString(keyFile), "the second run must not touch the key");
  }
}
