package im.redpanda.updater;

import im.redpanda.core.LocalSettings;
import im.redpanda.identity.NodeId;
import im.redpanda.identity.crypt.AddressFormatException;
import im.redpanda.identity.crypt.Base58;
import im.redpanda.identity.crypt.Sha256Hash;
import im.redpanda.identity.crypt.Utils;
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
            logger.warn(
                "update channel fail-closed: no production updater key configured ({})",
                e.toString());
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
   * System property overriding {@link #signingKeyPath()}. Tests only, and for the same reason as
   * the {@code redpanda.update.*.path} properties in {@link UpdateTransfer}: Surefire runs 8 forks
   * out of one working directory, so a test that touches the CWD-relative key file races the other
   * forks — and this particular file is a private signing key, which has no business being written
   * into a checkout by a test run at all.
   */
  static final String SIGNING_KEY_PATH_PROPERTY = "redpanda.updater.signing.key.path";

  /** The signing key file the key ceremony writes and the update-inserting step reads. */
  static Path signingKeyPath() {
    String configured = System.getProperty(SIGNING_KEY_PATH_PROPERTY);
    return Path.of(
        configured == null || configured.isBlank() ? "privateSigningKey.txt" : configured);
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

    if (!signingKeyPath().toFile().exists()) {
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

  /**
   * Offline key ceremony (T13): generates a fresh update-signing identity, writes the private half
   * to {@code privateSigningKey.txt} and prints the public half for the operator to paste into
   * {@link #PUBLIC_SIGNING_KEY_OF_CORE_DEVELOPERS}.
   *
   * <p>The key file is created exclusively, so an existing signing key is never overwritten — not
   * even by two ceremonies racing each other. Its permissions are {@code 0600} from creation on
   * POSIX filesystems and best-effort elsewhere (a non-POSIX filesystem simply has nothing to set);
   * the private key is never printed, so a filesystem without permissions still does not leak it
   * into a log.
   *
   * <p>The paste is deliberately manual (T121/TD131). Until 2026-09-04 a {@code @Test}-annotated
   * class {@code im.redpanda.core.SecureKeyGenerator} did all three steps automatically — generate,
   * write the private key into the CWD, and rewrite the constant in this very source file. It
   * escaped Surefire only because its class name misses the default include patterns, so a rename
   * to {@code *Test} would have armed a live key-rewriting test that silently swaps the network's
   * update-signing key. Rewriting the trust anchor is a decision, not a build step; the two steps
   * that are safe to automate live here, the one that is not stays a human edit.
   */
  public static void createNewKeys() {

    Path keyFile = signingKeyPath();
    if (Files.exists(keyFile)) {
      // Never overwrite an existing signing key (accidental key loss during the key
      // ceremony); move the old file away first if a new key is really intended.
      System.out.println(
          "Refusing to create new keys: " + keyFile.toAbsolutePath() + " already exists.");
      return;
    }

    NodeId nodeId = new NodeId();

    // The public key is printed only once the private half is safely on disk (see below): on the
    // race path and on any IO failure the generated key is discarded, and an operator who had
    // already seen a "Pub:" line could paste a trust anchor whose private key exists nowhere.
    // The private key must never be written to stdout (it may end up in logs);
    // write it to the file insertNewUpdate() reads, owner-readable only.
    try {
      try {
        // Create with 0600 upfront so the key is never world-readable, not even
        // between creation and the setPosixFilePermissions below. createFile is
        // exclusive, and its FileAlreadyExistsException is the guard that actually
        // holds: the Files.exists() check above can only refuse a file that was
        // already there when we looked, and swallowing the exception here would let
        // the writeString below truncate a signing key that appeared in between.
        Files.createFile(
            keyFile,
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
      } catch (UnsupportedOperationException e) {
        // non-POSIX filesystem (e.g. Windows): create exclusively anyway, without the
        // permission attribute; there is nothing to apply below either.
        Files.createFile(keyFile);
      }
      Files.writeString(keyFile, Base58.encode(nodeId.exportWithPrivate()));
      try {
        Files.setPosixFilePermissions(keyFile, PosixFilePermissions.fromString("rw-------"));
      } catch (UnsupportedOperationException ignored) {
        // non-POSIX filesystem (e.g. Windows); file is still not printed anywhere
      }
      System.out.println("Pub: " + Base58.encode(nodeId.exportPublic()));
      System.out.println("Priv: written to " + keyFile.toAbsolutePath());
      System.out.println(
          "Next step is manual on purpose: paste the Pub value above into"
              + " Updater.PUBLIC_SIGNING_KEY_OF_CORE_DEVELOPERS, rebuild, and roll the new jar out"
              + " before signing anything with this key.");
    } catch (FileAlreadyExistsException e) {
      // A key file appeared between the check above and the exclusive create. Keep the existing
      // key; the one generated here is discarded unwritten.
      System.out.println(
          "Refusing to create new keys: " + keyFile.toAbsolutePath() + " appeared meanwhile.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void insertNewUpdate() throws IOException, AddressFormatException {

    // lets test if we have the priv key before generating update
    String keyString = new String(Files.readAllBytes(signingKeyPath()));
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

  /**
   * System property naming the freshly built apk that {@link #insertNewAndroidUpdate()} signs and
   * moves into place. Defaults to {@link #DEFAULT_ANDROID_APK_SOURCE}.
   */
  public static final String ANDROID_APK_SOURCE_PROPERTY = "redpanda.android.apk.source";

  /**
   * Where a Flutter release build puts the apk, relative to this repository next to the mobile
   * checkout.
   *
   * <p>TD130: this used to be spelled with backslashes ({@code ..\app\build\...}), which is a
   * single path segment with backslashes in its name on Linux — so {@code lastModified()} returned
   * 0 and the read failed on every non-Windows signing host, including the one that actually signs
   * the testnet releases. Forward slashes work on both platforms.
   */
  public static final String DEFAULT_ANDROID_APK_SOURCE =
      "../app/build/app/outputs/apk/release/app-release.apk";

  /**
   * The apk {@link #insertNewAndroidUpdate()} signs: {@value #ANDROID_APK_SOURCE_PROPERTY} if set,
   * otherwise {@link #DEFAULT_ANDROID_APK_SOURCE}.
   */
  static Path androidApkSource() {
    String configured = System.getProperty(ANDROID_APK_SOURCE_PROPERTY);
    return Path.of(
        configured == null || configured.isBlank() ? DEFAULT_ANDROID_APK_SOURCE : configured);
  }

  /** Signs the apk named by {@value #ANDROID_APK_SOURCE_PROPERTY} (or the default). */
  public static void insertNewAndroidUpdate() throws IOException, AddressFormatException {
    insertNewAndroidUpdate(androidApkSource());
  }

  /**
   * Signs {@code source} with the local signing key, records timestamp and signature in the
   * uploader node's settings and moves the apk to the file the node distributes ({@link
   * UpdateTransfer#updateApkPath()}).
   */
  public static void insertNewAndroidUpdate(Path source)
      throws IOException, AddressFormatException {

    System.out.println("inserting " + source + " as android update...");
    // lets test if we have the priv key before generating update
    String keyString = new String(Files.readAllBytes(Path.of("privateSigningKey.txt")));
    keyString = keyString.replace("\n", "").replace("\r", "");

    NodeId nodeId = NodeId.importWithPrivate(Base58.decode(keyString));

    System.out.println("public key encoded: " + Base58.encode(nodeId.exportPublic()));

    long timestamp = source.toFile().lastModified();

    byte[] data = Files.readAllBytes(source);

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

    Path destination = UpdateTransfer.updateApkPath();
    System.out.println("renaming file to " + destination + " to be used from the client");
    Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
  }
}
