package im.redpanda.core;

import im.redpanda.crypt.AddressFormatException;
import im.redpanda.crypt.Base58;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.Security;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Updater {

  private static final Logger logger = LogManager.getLogger();

  /**
   * Build-time floor for accepted update timestamps (rollback protection). Raise this to the
   * release-signing timestamp with every signed release (see updater key-ceremony runbook in the
   * docs repo). Updates with timestamp &lt;= this value are rejected even on a fresh LocalSettings.
   */
  public static final long MIN_UPDATE_TIMESTAMP_MS = 1783728000000L; // 2026-07-11T00:00:00Z

  /**
   * Base58 of the 64-byte MS03 public NodeId export ([32 Ed25519 verify key][32 X25519 key]) of the
   * core developers' update-signing identity. Regenerated for MS03 — pre-MS03 (brainpool) update
   * signatures are no longer accepted.
   *
   * <p>INTERIM TESTNET KEY (2026-07-11): generated for the first real v23 network; to be rotated by
   * the human key ceremony (T13) once the updater hardening (T10) has landed.
   */
  public static final String PUBLIC_SIGNING_KEY_OF_CORE_DEVELOPERS =
      "pSX1GUpVfPuNUPvC5LZZQtRyt1f8xk9JvnYeWocXtVEgeXNwK3VPQe626HmA45af9zipa47W5gu26wnJT19FMaQ";

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  /**
   * Lazily-decoded, cached updater public key. {@code null} both before the first lookup and after
   * a failed decode (fail-closed) — {@link #decoded} distinguishes the two so the warning is only
   * logged once.
   */
  private static volatile NodeId cachedPublicUpdaterKey;

  private static volatile boolean decoded;

  public static NodeId getPublicUpdaterKey() {
    if (!decoded) {
      synchronized (Updater.class) {
        if (!decoded) {
          try {
            cachedPublicUpdaterKey =
                NodeId.importPublic(Base58.decode(PUBLIC_SIGNING_KEY_OF_CORE_DEVELOPERS));
          } catch (AddressFormatException | IllegalArgumentException e) {
            logger.warn("update channel fail-closed: no production updater key configured");
            cachedPublicUpdaterKey = null;
          }
          decoded = true;
        }
      }
    }
    return cachedPublicUpdaterKey;
  }

  /** Test-only override of the cached updater key; bypasses the normal decode path. */
  static void setPublicUpdaterKeyForTests(NodeId key) {
    synchronized (Updater.class) {
      cachedPublicUpdaterKey = key;
      decoded = true;
    }
  }

  /** Test-only reset of the lazy-holder cache so the next call re-decodes normally. */
  static void resetPublicUpdaterKeyForTests() {
    synchronized (Updater.class) {
      cachedPublicUpdaterKey = null;
      decoded = false;
    }
  }

  /**
   * This method is the entry point for the maven target "package".
   *
   * @param args
   */
  public static void main(String[] args) {
    if (args.length > 0 && "--create-keys".equals(args[0])) {
      // CLI entry point for the offline key ceremony (T13) — never invoked from CI/build.
      createNewKeys();
      return;
    }

    if (!Path.of("privateSigningKey.txt").toFile().exists()) {
      System.out.println("No private key for signing found, skipping insert update into network.");
      return;
    }

    System.out.println("Starting update inserting process...");

    try {
      insertNewUpdate();
      System.out.println(
          "Update was successfully signed and inserted in the defaul client for upload.");
    } catch (IOException e) {
      e.printStackTrace();
    } catch (AddressFormatException e) {
      e.printStackTrace();
    }

    try {
      insertNewAndroidUpdate();
      System.out.println(
          "Update of android.apk was successfully signed and inserted in the defaul client for upload.");
    } catch (java.nio.file.NoSuchFileException e) {
      System.out.println("No android.apk found, not inserting any android update...");
    } catch (IOException e) {
      e.printStackTrace();
    } catch (AddressFormatException e) {
      e.printStackTrace();
    }
  }

  public static void createNewKeys() {

    NodeId nodeId = new NodeId();

    System.out.println("Pub: " + Base58.encode(nodeId.exportPublic()));
    // The private key must never be written to stdout (it may end up in logs);
    // write it to the file insertNewUpdate() reads, owner-readable only.
    Path keyFile = Path.of("privateSigningKey.txt");
    try {
      try {
        // Create with 0600 upfront so the key is never world-readable, not even
        // between creation and the setPosixFilePermissions below.
        Files.createFile(
            keyFile,
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
      } catch (FileAlreadyExistsException | UnsupportedOperationException ignored) {
        // pre-existing file or non-POSIX filesystem; permissions re-applied below
      }
      Files.writeString(keyFile, Base58.encode(nodeId.exportWithPrivate()));
      try {
        Files.setPosixFilePermissions(keyFile, PosixFilePermissions.fromString("rw-------"));
      } catch (UnsupportedOperationException ignored) {
        // non-POSIX filesystem (e.g. Windows); file is still not printed anywhere
      }
      System.out.println("Priv: written to " + keyFile.toAbsolutePath());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void insertNewUpdate() throws IOException, AddressFormatException {

    // lets test if we have the priv key before generating update
    String keyString = new String(Files.readAllBytes(Path.of("privateSigningKey.txt")));
    keyString = keyString.replace("\n", "").replace("\r", "");

    NodeId nodeId = NodeId.importWithPrivate(Base58.decode(keyString));

    System.out.println("public key encoded: " + Base58.encode(nodeId.exportPublic()));

    File file = new File("target/redpanda.jar");

    long timestamp = file.lastModified();

    System.out.println("timestamp : " + timestamp);

    Path path = Path.of("target/redpanda.jar");
    byte[] data = Files.readAllBytes(path);

    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(timestamp);
    toHash.put(data);

    byte[] signature = nodeId.sign(toHash.array());

    System.out.println("signature len: " + signature.length);

    System.out.println("timestamp: " + timestamp);

    System.out.println("signature: " + Utils.bytesToHexString(signature));

    LocalSettings localSettings = LocalSettings.load(59558);

    localSettings.setUpdateSignature(signature);
    localSettings.setUpdateTimestamp(timestamp);
    localSettings.save(59558);
    System.out.println("saved in local settings!");

    System.out.println("verified: " + getPublicUpdaterKey().verify(toHash.array(), signature));

    System.out.println("hash: " + Sha256Hash.create(toHash.array()));
  }

  public static void insertNewAndroidUpdate() throws IOException, AddressFormatException {

    System.out.println("inserting android.apk as android update...");
    // lets test if we have the priv key before generating update
    String keyString = new String(Files.readAllBytes(Path.of("privateSigningKey.txt")));
    keyString = keyString.replace("\n", "").replace("\r", "");

    NodeId nodeId = NodeId.importWithPrivate(Base58.decode(keyString));

    System.out.println("public key encoded: " + Base58.encode(nodeId.exportPublic()));

    String fileName = "..\\app\\build\\app\\outputs\\apk\\release\\app-release.apk";

    File file = new File(fileName);

    long timestamp = file.lastModified();

    Path path = Path.of(fileName);
    byte[] data = Files.readAllBytes(path);

    ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
    toHash.putLong(timestamp);
    toHash.put(data);

    byte[] signature = nodeId.sign(toHash.array());

    System.out.println("signature len: " + signature.length);

    System.out.println("timestamp: " + timestamp);

    System.out.println("signature: " + Utils.bytesToHexString(signature));

    LocalSettings localSettings = LocalSettings.load(59558);

    localSettings.setUpdateAndroidSignature(signature);
    localSettings.setUpdateAndroidTimestamp(timestamp);
    localSettings.save(59558);
    System.out.println("saved in local settings!");

    System.out.println("verified: " + getPublicUpdaterKey().verify(toHash.array(), signature));

    System.out.println("hash: " + Sha256Hash.create(toHash.array()));

    System.out.println("renaming file to android.apk to be used from the client");

    Path source = Path.of(fileName);
    Files.move(source, Path.of("android.apk"), StandardCopyOption.REPLACE_EXISTING);
  }
}
