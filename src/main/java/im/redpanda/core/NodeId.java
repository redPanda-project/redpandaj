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

public class NodeId {

    KeyPair keyPair;
    KademliaId kademliaId;


    public NodeId() {
        keyPair = generateECKeys();
    }

    public NodeId(KeyPair keyPair) {
        this.keyPair = keyPair;
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
        System.out.println("len: " + buffer);
        return buffer.array();
    }

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
}
