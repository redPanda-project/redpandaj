package im.redpanda.flaschenpost;

import im.redpanda.core.KademliaId;
import im.redpanda.core.Log;
import im.redpanda.core.NodeId;
import im.redpanda.core.Server;
import im.redpanda.crypt.Utils;

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
    public static final int IV_LEN = 16;

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
    private byte[] signature;

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


    public GarlicMessage(KademliaId destination, ByteBuffer buffer) {
        //KademliaId kademliaId = KademliaId.fromBuffer(buffer);

        super(destination);


        byte[] ivBytes = new byte[IV_LEN];
        buffer.get(ivBytes);
        iv = ivBytes;

        NodeId pubkeyForEncryption = NodeId.fromBufferGetPublic(buffer);
        encryptionNodeId = pubkeyForEncryption;


        int encryptedLength = buffer.getInt();
        byte[] contentBytes = new byte[encryptedLength];
        buffer.get(contentBytes);

        setContent(contentBytes);

        signature = Utils.readSignature(buffer);

        nestedMessages = new ArrayList<>();

    }

    public void addGMContent(GMContent gmContent) {
        nestedMessages.add(gmContent);
    }

    @Override
    protected void computeContent() {

        int bytesForContent = 0;
        for (GMContent c : nestedMessages) {
            bytesForContent += 4;
            bytesForContent += c.getContent().length;
        }

//        int dataLen = iv.length + 4 + bytesForContent;
//        int bufferWithoutSignatureLen = 1 + 4 + KademliaId.ID_LENGTH_BYTES + dataLen;
//        ByteBuffer contentToEncrypt = ByteBuffer.allocate(bufferWithoutSignatureLen);


        ByteBuffer contentToEncrypt = ByteBuffer.allocate(4 + bytesForContent);
        contentToEncrypt.putInt(nestedMessages.size());
        for (GMContent c : nestedMessages) {
            byte[] content = c.getContent();
            contentToEncrypt.putInt(content.length);
            contentToEncrypt.put(content);
        }


        if (contentToEncrypt.position() != contentToEncrypt.limit()) {
            throw new RuntimeException("contentToEncrypt has wrong size: " + contentToEncrypt.position() + " " + contentToEncrypt.limit());
        }

        SecretKey sharedSecret = getSharedSecret(encryptionNodeId, targetsNodeId);

        IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);

        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM, PROVIDER);
            cipher.init(Cipher.ENCRYPT_MODE, sharedSecret, ivParameterSpec);
            byte[] encryptedBytes = cipher.doFinal(contentToEncrypt.array());

            byte[] signature = encryptionNodeId.sign(encryptedBytes);

            ByteBuffer encryptedAndSignedBytes = ByteBuffer.allocate(1 + IV_LEN + NodeId.PUBLIC_KEYLEN + 4 + encryptedBytes.length + signature.length);
            encryptedAndSignedBytes.put(getGMType().getId());
            encryptedAndSignedBytes.put(iv);
            encryptedAndSignedBytes.put(encryptionNodeId.exportPublic());
            encryptedAndSignedBytes.putInt(encryptedBytes.length);
            encryptedAndSignedBytes.put(encryptedBytes);
            encryptedAndSignedBytes.put(signature);

            if (encryptedAndSignedBytes.position() != encryptedAndSignedBytes.limit()) {
                throw new RuntimeException("contentToEncrypt has wrong size: " + encryptedAndSignedBytes.position() + " " + encryptedAndSignedBytes.limit());
            }

            setContent(encryptedAndSignedBytes.array());


            System.out.println("set content!");

        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException e) {
            Log.sentry(e);
            e.printStackTrace();
        }
    }

    protected void parseContent() {

        //lets check the signature
        boolean verify = encryptionNodeId.verify(getContent(), signature);
        if (!verify) {
            System.out.println("signature could not be verified for a Garlic Message...");
            Log.sentry("signature could not be verified for a Garlic Message...");
            return;
        }


        //lets decrypt the content
        SecretKey sharedSecret = getSharedSecret(Server.nodeId, encryptionNodeId);

        IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);

        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM, PROVIDER);
            cipher.init(Cipher.DECRYPT_MODE, sharedSecret, ivParameterSpec);
            byte[] decryptedBytes = cipher.doFinal(getContent());

            ByteBuffer decryptedBuffer = ByteBuffer.wrap(decryptedBytes);
            int numberOfNeastedMessages = decryptedBuffer.getInt();


            for (int i = 0; i < numberOfNeastedMessages; i++) {

                int toParseBytes = decryptedBuffer.getInt();
                int startingPosition = decryptedBuffer.position();
                GMContent parsed = GMParser.parse(decryptedBuffer);
                addGMContent(parsed);

                if (decryptedBuffer.position() != startingPosition + toParseBytes) {
                    throw new RuntimeException("nested messages of garlic message could not be parsed correctly...");
                }

            }

        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException e) {
            Log.sentry(e);
            e.printStackTrace();
        }

    }


    private SecretKey getSharedSecret(NodeId priv, NodeId pub) {
        try {
            KeyAgreement keyAgreement = KeyAgreement.getInstance("ECDH", "BC");
            keyAgreement.init(priv.getKeyPair().getPrivate());
            keyAgreement.doPhase(pub.getKeyPair().getPublic(), true);

            SecretKey intermediateSharedSecret = keyAgreement.generateSecret("AES");

            System.out.println("shared secret: " + Utils.bytesToHexString(intermediateSharedSecret.getEncoded()));

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
