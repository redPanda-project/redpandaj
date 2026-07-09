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

public class Updater {

  /**
   * Base58 of the 64-byte MS03 public NodeId export ([32 Ed25519 verify key][32 X25519 key]) of the
   * core developers' update-signing identity. Regenerated for MS03 — pre-MS03 (brainpool) update
   * signatures are no longer accepted.
   */
  public static final String PUBLIC_SIGNING_KEY_OF_CORE_DEVELOPERS = "MS03_PLACEHOLDER_PUBLIC_KEY";

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  public static NodeId getPublicUpdaterKey() {
    try {
      return NodeId.importPublic(Base58.decode(PUBLIC_SIGNING_KEY_OF_CORE_DEVELOPERS));
    } catch (AddressFormatException | IllegalArgumentException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * This method is the entry point for the maven target "package".
   *
   * @param args
   */
  public static void main(String[] args) {
    // createNewKeys();

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
