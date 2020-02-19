package im.redpanda.core;

import im.redpanda.crypt.Sha256Hash;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;

import java.nio.ByteBuffer;
import java.security.*;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

/**
 * This class represents a NodeId for every Peer in the network. This is an ellipic curve diffie hellman key,
 * where the public key is required and the private key is optional. The associated KademliaId is computed from a SHA256
 * hash of the public key. We use HashCash to make the computation of many valid keys costy.
 * <p>
 * The curve 'brainpoolp256r1' may change later
 * as well as the import and export methods.
 */
public class NodeId {


    public static final int PUBLIC_KEYLEN = 92;
    public static final int PRIVATE_KEYLEN = 252;

    KeyPair keyPair;
    KademliaId kademliaId;

    /**
     * Generates a new NodeId from a ECDH keypair. The KademliaId is automatically computed when calling the get method.
     *
     * @param keyPair
     */
    public NodeId(KeyPair keyPair) {
        this.keyPair = keyPair;
    }

    /**
     * Generates a new NodeId with a new random key.
     */
    public NodeId() {
        if (!Log.isJUnitTest()) {
            System.out.println("generating new node id, this may take some time");
        }
        while (true) {
//            System.out.print(".");
            keyPair = generateECKeys();
            Sha256Hash sha256Hash = Sha256Hash.createDouble(keyPair.getPublic().getEncoded());
            byte[] bytes = sha256Hash.getBytes();

//            if (bytes[0] == 0 && bytes[1] == 0) {

            if (Log.isJUnitTest()) {
                break;
            }

            if (bytes[0] == 0) {
                //todo change later for prod to more 0's
                /**
                 * This key is valid
                 */
                break;
            }
        }
    }

    /**
     * Checks if this NodeId satisfies the required property
     * (first byte from SHA256-double from public key bytes should be zero).
     *
     * @return
     */
    public boolean checkValid() {
        Sha256Hash sha256Hash = Sha256Hash.createDouble(keyPair.getPublic().getEncoded());
        byte[] bytes = sha256Hash.getBytes();
        return bytes[0] == 0;
    }


    public static KeyPair generateECKeys() {
        try {
            ECNamedCurveParameterSpec parameterSpec = ECNamedCurveTable.getParameterSpec("brainpoolp256r1");
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(
                    "ECDH", "BC");

            keyPairGenerator.initialize(parameterSpec);
            KeyPair keyPair = keyPairGenerator.generateKeyPair();

            return keyPair;
        } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException
                | NoSuchProviderException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static NodeId importWithPrivate(byte[] bytes) {


        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int len = buffer.getInt();
        byte[] privateKeyBytes = new byte[len];
        buffer.get(privateKeyBytes);
        EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(privateKeyBytes);


        len = buffer.getInt();
        byte[] publicKeyBytes = new byte[len];
        buffer.get(publicKeyBytes);
        EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyBytes);

        KeyFactory keyFactory = null;
        try {
            keyFactory = KeyFactory.getInstance("ECDH", "BC");
            PrivateKey newPrivateKey = keyFactory.generatePrivate(privateKeySpec);
            PublicKey newPublicKey = keyFactory.generatePublic(publicKeySpec);

            KeyPair keyPair = new KeyPair(newPublicKey, newPrivateKey);
            return new NodeId(keyPair);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchProviderException e) {
            e.printStackTrace();
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * The KademliaId is automatically computed upon the first call of this method.
     *
     * @return
     */
    public KademliaId getKademliaId() {
        if (kademliaId == null) {
            kademliaId = fromPublicKey(keyPair.getPublic());
        }
        return kademliaId;
    }

    public KademliaId fromPublicKey(PublicKey key) {
        Sha256Hash sha256Hash = Sha256Hash.create(key.getEncoded());
        return KademliaId.fromFirstBytes(sha256Hash.getBytes());
    }

    public byte[] exportWithPrivate() {
        ByteBuffer buffer = ByteBuffer.allocate(252);
        byte[] encoded = keyPair.getPrivate().getEncoded();
        buffer.putInt(encoded.length);
        buffer.put(encoded);

        encoded = keyPair.getPublic().getEncoded();
        buffer.putInt(encoded.length);
        buffer.put(encoded);
        return buffer.array();
    }

    public byte[] exportPublic() {
        //Todo: save the value after first creation... speeed!
        ByteBuffer buffer = ByteBuffer.allocate(PUBLIC_KEYLEN);
        byte[] encoded = keyPair.getPublic().getEncoded();
        buffer.put(encoded);
        return buffer.array();
    }

    public static NodeId importPublic(byte[] bytes) {
        EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(bytes);

        KeyFactory keyFactory = null;
        try {
            keyFactory = KeyFactory.getInstance("ECDH", "BC");
            PublicKey newPublicKey = keyFactory.generatePublic(publicKeySpec);

            KeyPair keyPair = new KeyPair(newPublicKey, null);
            return new NodeId(keyPair);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (NoSuchProviderException e) {
            e.printStackTrace();
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * Two NodeId are equal if their KademliaId is equal.
     *
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }

        if (obj instanceof NodeId) {
            return getKademliaId().equals(((NodeId) obj).getKademliaId());
        }

        return false;
    }

    public KeyPair getKeyPair() {
        return keyPair;
    }
}
