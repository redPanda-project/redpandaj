package im.redpanda.core;

import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.Utils;
import io.sentry.Sentry;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;

import java.io.*;
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
public class NodeId implements Serializable {


    public static final int PUBLIC_KEYLEN_LONG = 92;
    public static final int PUBLIC_KEYLEN = 65;
    public static final int PRIVATE_KEYLEN = 252;
    public static byte[] curveParametersASN1;

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
     * Obtains a NodeId object from a given KademliaId, the keypair is unknown.
     *
     * @param kademliaId
     */
    public NodeId(KademliaId kademliaId) {
        this.kademliaId = kademliaId;
    }

    /**
     * Generates a new NodeId with a new random key.
     */
    public NodeId() {
        if (!Log.isJUnitTest()) {
//            System.out.println("generating new node id, this may take some time");
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

    public boolean hasPrivate() {
        if (keyPair == null) {
            return false;
        }
        return keyPair.getPrivate() != null;
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

    public static KademliaId fromPublicKey(PublicKey key) {
        Sha256Hash sha256Hash = Sha256Hash.create(NodeId.exportPublic(key));

        byte[] bytes = sha256Hash.getBytes();

        return KademliaId.fromFirstBytes(bytes);
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
        return exportPublic(this.keyPair.getPublic());
    }


    public static byte[] exportPublic(PublicKey publicKey) {
        byte[] encoded = publicKey.getEncoded();

        //we need only the last 65 bytes since the first bytes are already given by the curve!

        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        byte[] bytes = new byte[PUBLIC_KEYLEN];
        buffer.position(PUBLIC_KEYLEN_LONG - PUBLIC_KEYLEN);
        buffer.get(bytes);

        return bytes;
    }

    public static NodeId importPublic(byte[] bytes) {

        byte[] bytesFull = new byte[PUBLIC_KEYLEN_LONG];
        ByteBuffer.wrap(bytesFull)
                .put(getCurveParametersForASN1Format())
                .put(bytes);


        EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(bytesFull);

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

    public static byte[] getCurveParametersForASN1Format() {
        if (curveParametersASN1 == null) {
            //generate the first bytes for the X209 ASN1 format
            PublicKey aPublic = generateECKeys().getPublic();
            ByteBuffer wrap = ByteBuffer.wrap(aPublic.getEncoded());
            byte[] bytes = new byte[PUBLIC_KEYLEN_LONG - PUBLIC_KEYLEN];
            wrap.get(bytes);
            curveParametersASN1 = bytes;
        }
        return curveParametersASN1;
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

    /**
     * Sets the keypair of this object if not already provided and will check against the KademliaId that this
     * keypair fits to the already provided KademliaId.
     *
     * @param keyPair
     */
    public void setKeyPair(KeyPair keyPair) throws KeypairDoesNotMatchException {
        if (keyPair != null) {
            throw new RuntimeException("keypair has to be null if you want to set the keypair of a NodeId!");
        }
        if (kademliaId == null) {
            throw new RuntimeException("To check the keypair there has to be already a known KademliaId!");
        }

        KademliaId kademliaId = fromPublicKey(keyPair.getPublic());

        if (!kademliaId.equals(this.kademliaId)) {
            throw new KeypairDoesNotMatchException();
        } else {
            this.keyPair = keyPair;
        }

    }

    public static class KeypairDoesNotMatchException extends Exception {

    }

    /**
     * Hashes the bytes with SHA256 and computes the signature in ASN.1 format, for more info see:
     * https://crypto.stackexchange.com/questions/1795/how-can-i-convert-a-der-ecdsa-signature-to-asn-1
     *
     * @param bytesToSign
     * @return
     */
    public byte[] sign(byte[] bytesToSign) {

        if (getKeyPair() == null || getKeyPair().getPrivate() == null) {
            throw new RuntimeException("this NodeId can not be used for signing since there is no private key!");
        }
        /*
         * Create a Signature object and initialize it with the private key
         */
        try {
            Signature ecdsa = Signature.getInstance("SHA256withECDSA");

            ecdsa.initSign(getKeyPair().getPrivate());

            ecdsa.update(bytesToSign);

            /*
             * Now that all the data to be signed has been read in, generate a
             * signature for it
             */

            byte[] realSig = ecdsa.sign();
//            System.out.println("Signature: " + Utils.bytesToHexString(realSig));

            return realSig;

        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            Log.sentry(e);
            e.printStackTrace();
        }

        return null;

    }

    /**
     * Verifies the bytes with the signature, this method uses SHA256withECDSA such that the bytes should not be
     * hashed by sha256 before using this method!
     *
     * @param bytesToVerify
     * @param signature
     * @return
     */
    public boolean verify(byte[] bytesToVerify, byte[] signature) {
        try {
            Signature ecdsa2 = Signature.getInstance("SHA256withECDSA");
            ecdsa2.initVerify(getKeyPair().getPublic());

            ecdsa2.update(bytesToVerify);

            return ecdsa2.verify(signature);
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            Log.sentry(e);
            e.printStackTrace();
        }
        return false;
    }

    private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
        boolean hasPrivate = aInputStream.readBoolean();

        byte[] bytes = new byte[hasPrivate ? PRIVATE_KEYLEN : PUBLIC_KEYLEN];
        aInputStream.read(bytes);

        NodeId nodeId;
        if (hasPrivate) {
            nodeId = importWithPrivate(bytes);
        } else {
            nodeId = importPublic(bytes);
        }
        keyPair = nodeId.getKeyPair();
    }

    private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
        aOutputStream.writeBoolean(hasPrivate());
        if (hasPrivate()) {
            aOutputStream.write(exportWithPrivate());
        } else {
            aOutputStream.write(exportPublic());
        }
    }

    @Override
    public String toString() {
        return getKademliaId().toString();
    }


    public static NodeId fromBufferGetPublic(ByteBuffer buffer) {
        byte[] publicKeyBytes = new byte[NodeId.PUBLIC_KEYLEN];
        buffer.get(publicKeyBytes);

        return NodeId.importPublic(publicKeyBytes);
    }

}

