package im.redpanda.core;

import im.redpanda.crypt.AddressFormatException;
import im.redpanda.crypt.Base58;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.Security;

public class Updater {


    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    public static NodeId getPublicUpdaterKey() {
        try {
            return NodeId.importPublic(Base58.decode("N3Zu35JfCBtt3d9AfoUqgkrLQa7y4t462ZBfF2bGrLM1bdhWu6WaieEKYjx93YeaWh66xSqmD7c3MTCMTzSHSe3J"));
        } catch (AddressFormatException e) {
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
//        createNewKeys();

        if (!Paths.get("privateSigningKey.txt").toFile().exists()) {
            System.out.println("No private key for signing found, skipping insert update into network.");
            return;
        }

        System.out.println("Starting update inserting process...");

        try {
            insertNewUpdate();
            System.out.println("Update was successfully signed and inserted in the defaul client for upload.");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (AddressFormatException e) {
            e.printStackTrace();
        }


        try {
            insertNewAndroidUpdate();
            System.out.println("Update of android.apk was successfully signed and inserted in the defaul client for upload.");
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
        System.out.println("Priv: " + Base58.encode(nodeId.exportWithPrivate()) + "\n\n\n\n");

    }

    public static void insertNewUpdate() throws IOException, AddressFormatException {


        //lets test if we have the priv key before generating update
        String keyString = new String(Files.readAllBytes(Paths.get("privateSigningKey.txt")));
        keyString = keyString.replace("\n", "").replace("\r", "");
//        System.out.println("privKey: '" + keyString + "'");

        NodeId nodeId = NodeId.importWithPrivate(Base58.decode(keyString));

        System.out.println("public key encoded: " + Base58.encode(nodeId.exportPublic()));

        File file = new File("target/redpanda.jar");

        long timestamp = file.lastModified();

        System.out.println("timestamp : " + timestamp);

        Path path = Paths.get("target/redpanda.jar");
        byte[] data = Files.readAllBytes(path);

        int updateSize = data.length;


        ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
        toHash.putLong(timestamp);
        toHash.put(data);


        byte[] signature = nodeId.sign(toHash.array());

        System.out.println("signature len: " + signature.length + " " + ((int) signature[1] + 2));

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
        //lets test if we have the priv key before generating update
        String keyString = new String(Files.readAllBytes(Paths.get("privateSigningKey.txt")));
        keyString = keyString.replace("\n", "").replace("\r", "");
//        System.out.println("privKey: '" + keyString + "'");

        NodeId nodeId = NodeId.importWithPrivate(Base58.decode(keyString));

        System.out.println("public key encoded: " + Base58.encode(nodeId.exportPublic()));

        String fileName = "..\\app\\build\\app\\outputs\\apk\\release\\app-release.apk";

        File file = new File(fileName);

        long timestamp = file.lastModified();

        System.out.println("timestamp : " + timestamp + " ago: " + (System.currentTimeMillis() - timestamp));

        Path path = Paths.get(fileName);
        byte[] data = Files.readAllBytes(path);

        int updateSize = data.length;


        ByteBuffer toHash = ByteBuffer.allocate(8 + data.length);
        toHash.putLong(timestamp);
        toHash.put(data);


        byte[] signature = nodeId.sign(toHash.array());

        System.out.println("signature len: " + signature.length + " " + ((int) signature[1] + 2));

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

        Path source = Paths.get(fileName);
//        Files.move(source, source.resolveSibling("android.apk"), StandardCopyOption.REPLACE_EXISTING);
        Files.move(source, Paths.get("android.apk"), StandardCopyOption.REPLACE_EXISTING);
    }

}
