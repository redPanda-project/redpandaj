package im.redpanda.flaschenpost;

import im.redpanda.core.KademliaId;
import im.redpanda.core.Log;
import im.redpanda.core.NodeId;
import im.redpanda.core.Server;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.ArrayList;

public class GarlicMessage extends Flaschenpost {

    public static final String ALGORITHM = "AES/CTR/NoPadding";
    public static final String PROVIDER = "SunJCE";

    /**
     * This is the {@link NodeId} containing the public key of the target {@link im.redpanda.core.Peer}/{@link im.redpanda.core.Node}
     * and is only used for the creation process.
     */
    private NodeId targetsNodeId;
    /**
     * The public key of the target.
     */
    private byte[] publicKey;
    /**
     * The NodeId used for encryption. This NodeId should not be reused!
     * There is always a new NodeId for every new garlic message.
     */
    private NodeId encryptionNodeId;
    /**
     * ackId which should be encrypted as well. This ackId is used to acknowledge the Flaschenpost.
     */
    private int ackId;
    /**
     * The Content of the GarlicMessage is a List of other GarlicMessages.
     */
    private byte[] iv;
    private ArrayList<GMContent> nestedMessages;

    public GarlicMessage(NodeId targetsNodeId) {
        // we create a Flaschenpost with the target KademliaId and a new random integer as id and the current time.
        super(targetsNodeId.getKademliaId());

        this.targetsNodeId = targetsNodeId;
        this.publicKey = targetsNodeId.exportPublic();
        this.ackId = Server.random.nextInt();
        this.nestedMessages = new ArrayList<>();
        this.iv = new byte[16];
        Server.secureRandom.nextBytes(this.iv);

        this.encryptionNodeId = new NodeId();
    }

    public void addGMContent(GMContent gmContent) {
        nestedMessages.add(gmContent);
    }

    @Override
    protected void computeContent() {

        int bytesForContent = 0;
        for (GMContent c : nestedMessages) {
            bytesForContent += c.getContent().length;
        }

        int dataLen = iv.length + 4 + bytesForContent;
        int bufferWithoutSignatureLen = 1 + 4 + KademliaId.ID_LENGTH_BYTES + dataLen;
        ByteBuffer bytebuffer = ByteBuffer.allocate(bufferWithoutSignatureLen);

        bytebuffer.put(getGMType().getId());
        bytebuffer.putInt(dataLen);
        bytebuffer.put(destination.getBytes());
        bytebuffer.put(iv);
        bytebuffer.putInt(nestedMessages.size());
        for (GMContent c : nestedMessages) {
            bytebuffer.put(c.getContent());
        }


        if (bytebuffer.position() != bytebuffer.limit()) {
            throw new RuntimeException("bytebuffer has wrong size: " + bytebuffer.position() + " " + bytebuffer.limit());
        }

        SecretKey sharedSecret = getSharedSecret();

        IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);

        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM, PROVIDER);
            cipher.init(Cipher.ENCRYPT_MODE, sharedSecret, ivParameterSpec);
            byte[] encryptedBytes = cipher.doFinal(bytebuffer.array());

            byte[] signature = targetsNodeId.sign(encryptedBytes);

            ByteBuffer encryptedAndSignedBytes = ByteBuffer.allocate(encryptedBytes.length + signature.length);
            encryptedAndSignedBytes.put(encryptedBytes);
            encryptedAndSignedBytes.put(signature);

            if (encryptedAndSignedBytes.position() != encryptedAndSignedBytes.limit()) {
                throw new RuntimeException("bytebuffer has wrong size: " + encryptedAndSignedBytes.position() + " " + encryptedAndSignedBytes.limit());
            }

            setContent(encryptedAndSignedBytes.array());


            System.out.println("set content!");

        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException e) {
            Log.sentry(e);
            e.printStackTrace();
        }
    }

    private SecretKey getSharedSecret() {
        try {
            KeyAgreement keyAgreement = KeyAgreement.getInstance("ECDH", "BC");
            keyAgreement.init(encryptionNodeId.getKeyPair().getPrivate());
            keyAgreement.doPhase(targetsNodeId.getKeyPair().getPublic(), true);

            SecretKey intermediateSharedSecret = keyAgreement.generateSecret("AES");

            return intermediateSharedSecret;
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeyException e) {
            Log.sentry(e);
            e.printStackTrace();
            return null;
        }

    }

    @Override
    public GMType getGMType() {
        return GMType.GARLIC_MESSAGE;
    }
}
