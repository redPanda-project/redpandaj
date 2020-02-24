package im.redpanda.core;

import im.redpanda.crypt.AddressFormatException;
import im.redpanda.crypt.Base58;
import im.redpanda.crypt.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.Security;

public class Updater {


    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    public static NodeId getPublicUpdaterKey() {
        try {
            return NodeId.importPublic(Base58.decode("3YdhcPz2PJZVKarxZAEsuX3YMCJK9FwJBzSpcW1KogypMRNEyLXHCbYXxSzDPA27mqHdpXZGJEqfjoxhtqFeLXj37J67HajEd2zaTwnbdjFjMKyBhLtor1fpAh4tgg"));
        } catch (AddressFormatException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        createNewKeys();

        try {
            insertNewUpdate();
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
        System.out.println("privKey: '" + keyString + "'");

        NodeId nodeId = NodeId.importWithPrivate(Base58.decode(keyString));


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

        System.out.println("signature len: " + signature.length + " " + ((int) signature[1]+2));

        System.out.println("signature: " + Utils.bytesToHexString(signature));

        LocalSettings localSettings = LocalSettings.load(59558);

        localSettings.setUpdateSignature(signature);
        localSettings.setUpdateTimestamp(timestamp);
        localSettings.save(59558);
        System.out.println("saved in local settings!");


    }

}
