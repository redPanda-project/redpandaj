package im.redpanda.core;

import im.redpanda.crypt.Base58;
import im.redpanda.crypt.Sha256Hash;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.*;

public class PeerInHandshake {

    String ip;
    int port = 0;
    int status = 0;
    KademliaId identity;
    NodeId nodeId;
    Peer peer;
    SocketChannel socketChannel;
    SelectionKey key;
    byte[] randomFromUs;
    byte[] randomFromThem;

    boolean weSendOurRandom = false;
    boolean awaitingEncryption = false;
    boolean encryptionActive = false;

    //Secret key and iv used for AES encryption
    SecretKey sharedSecret;
    IvParameterSpec ivSend;
    IvParameterSpec ivReceive;
    Cipher cipherSend;
    Cipher cipherReceive;


    public PeerInHandshake(String ip, SocketChannel socketChannel) {
        this.ip = ip;
        this.socketChannel = socketChannel;
    }

    public PeerInHandshake(String ip, Peer peer, SocketChannel socketChannel) {
        this.ip = ip;
        this.peer = peer;
        this.socketChannel = socketChannel;
    }

    /**
     * 0 default value, before any handshake was parsed.
     * <p></p>
     * 1 first handshake was parsed, here we are waiting to obtain more information of the peer like the public key
     * to finish the complete handshake.
     * 2 do not connect, connected to ourselves or blacklisted
     * -1 handshake finished from our site, we do not expect more data before switching to encryption.
     * We are waiting for the switching byte to start the encryption.
     *
     * @param status
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * For the status information see the setter method.
     *
     * @return
     */
    public int getStatus() {
        return status;
    }

    public PeerInHandshake(String ip) {
        this.ip = ip;
    }


    public void addConnection() {
        try {
            socketChannel.configureBlocking(false);

            SelectionKey key = null;
            ConnectionHandler.selectorLock.lock();
            try {
                ConnectionHandler.selector.wakeup();
//                if (connectionPending) {
//                    peer.isConnecting = true;
//                    peer.setConnected(false);
                key = socketChannel.register(ConnectionHandler.selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
//                } else {
//                    peer.isConnecting = false;
//                    peer.setConnected(true);
//                    key = socketChannel.register(ConnectionHandler.selector, SelectionKey.OP_READ);
//                }
            } finally {
                ConnectionHandler.selectorLock.unlock();
            }


            key.attach(this);
            this.key = key;

//            peer.setSelectionKey(key);
            ConnectionHandler.selector.wakeup();
            Log.putStd("added con");
        } catch (IOException ex) {
            ex.printStackTrace();
            peer.disconnect("could not init connection....");
            return;
        }
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public KademliaId getIdentity() {
        return identity;
    }

    public void setIdentity(KademliaId nonce) {
        this.identity = nonce;
    }

    public Peer getPeer() {
        return peer;
    }

    public void setPeer(Peer peer) {
        this.peer = peer;
    }

    public SelectionKey getKey() {
        return key;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public void setNodeId(NodeId nodeId) {
        this.nodeId = nodeId;
    }

    public byte[] getRandomFromUs() {

        if (randomFromUs == null) {
            byte[] randomBytesForEncryption = new byte[16];
            Server.secureRandom.nextBytes(randomBytesForEncryption);
            randomFromUs = randomBytesForEncryption;
        }

        return randomFromUs;
    }


    public byte[] getRandomFromThem() {
        return randomFromThem;
    }

    public void setRandomFromThem(byte[] randomFromThem) {
        this.randomFromThem = randomFromThem;
    }

    public void calculateSharedSecret() {

        try {
            KeyAgreement keyAgreement = KeyAgreement.getInstance("ECDH", "BC");
            keyAgreement.init(Server.nodeId.getKeyPair().getPrivate());
            keyAgreement.doPhase(nodeId.getKeyPair().getPublic(), true);

            SecretKey intermediateSharedSecret = keyAgreement.generateSecret("AES");

            byte[] encoded = intermediateSharedSecret.getEncoded();


            ByteBuffer bytesForPrivateAESkey = ByteBuffer.allocate(32 + 16 + 16);

            bytesForPrivateAESkey.put(encoded);

            if (peerIsHigher()) {
                bytesForPrivateAESkey.put(randomFromUs);
                bytesForPrivateAESkey.put(randomFromThem);
            } else {
                bytesForPrivateAESkey.put(randomFromThem);
                bytesForPrivateAESkey.put(randomFromUs);
            }

            if (bytesForPrivateAESkey.remaining() != 0) {
                throw new RuntimeException("here is something wrong with the random bytes length!");
            }


            Sha256Hash sha256Hash = Sha256Hash.create(bytesForPrivateAESkey.array());

            sharedSecret = new SecretKeySpec(sha256Hash.getBytes(), "AES");

            System.out.println("asf " + Base58.encode(sharedSecret.getEncoded()));


            ByteBuffer bytesForIVsend = ByteBuffer.allocate(16 + 16);
            ByteBuffer bytesForIVreceive = ByteBuffer.allocate(16 + 16);

            bytesForIVsend.put(randomFromUs);
            bytesForIVsend.put(randomFromThem);
            bytesForIVreceive.put(randomFromThem);
            bytesForIVreceive.put(randomFromUs);

            //todo: iv are just the way around for send/receive, is this a security risk?
            ivSend = new IvParameterSpec(bytesForIVsend.array());
            System.out.println("send iv: " + Base58.encode(bytesForIVsend.array()));
            ivReceive = new IvParameterSpec(bytesForIVreceive.array());
            System.out.println("rec iv: " + Base58.encode(bytesForIVreceive.array()));

        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchProviderException e) {
            e.printStackTrace();
        }

    }

    public boolean peerIsHigher() {
//        return isConnectionInitializedByMe;
        for (int i = 0; i < KademliaId.ID_LENGTH / 8; i++) {
            int compare = Byte.compare(Server.NONCE.getBytes()[i], nodeId.getKademliaId().getBytes()[i]);
            if (compare > 0) {
                return true;
            } else if (compare < 0) {
                return false;
            }
        }
        System.out.println("could not compare!!!");
        return false;
    }

    public boolean isWeSendOurRandom() {
        return weSendOurRandom;
    }

    public void setWeSendOurRandom(boolean weSendOurRandom) {
        this.weSendOurRandom = weSendOurRandom;
    }

    public boolean isAwaitingEncryption() {
        return awaitingEncryption;
    }

    public void setAwaitingEncryption(boolean awaitingEncryption) {
        this.awaitingEncryption = awaitingEncryption;
    }

    public boolean hasPublicKey() {
        return getPeer().getNodeId() != null;
    }

    public boolean isEncryptionActive() {
        return encryptionActive;
    }

    public void activateEncryption() {
        encryptionActive = true;
        try {
            //let set up the send Cipher
            cipherSend = Cipher.getInstance("AES/GCM/NoPadding", "BC");
            cipherSend.init(Cipher.ENCRYPT_MODE, sharedSecret, ivSend);


            //let set up the receive Cipher
            cipherReceive = Cipher.getInstance("AES/GCM/NoPadding", "BC");
            cipherReceive.init(Cipher.DECRYPT_MODE, sharedSecret, ivReceive);


        } catch (NoSuchAlgorithmException | NoSuchProviderException
                | NoSuchPaddingException | InvalidKeyException
                | InvalidAlgorithmParameterException e) {
            e.printStackTrace();
        }
    }

    public byte[] encrypt(byte[] toEncrypt) {

        try {

            byte[] outputEncryptedBytes;

            outputEncryptedBytes = new byte[cipherSend.getOutputSize(toEncrypt.length)];
            int encryptLength = cipherSend.update(toEncrypt, 0,
                    toEncrypt.length, outputEncryptedBytes, 0);
            encryptLength += cipherSend.doFinal(outputEncryptedBytes, encryptLength);


            return outputEncryptedBytes;
        } catch (ShortBufferException
                | IllegalBlockSizeException | BadPaddingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public byte[] decrypt(byte[] bytesToDecrypt) {
        try {
            byte[] outPlain;

            System.out.println("len to decrypt: " + bytesToDecrypt.length);

            outPlain = new byte[cipherReceive.getOutputSize(bytesToDecrypt.length)];
            int decryptLength = cipherReceive.update(bytesToDecrypt, 0,
                    bytesToDecrypt.length, outPlain, 0);
            decryptLength += cipherReceive.doFinal(outPlain, decryptLength);

            return outPlain;
        } catch (IllegalBlockSizeException | BadPaddingException
                | ShortBufferException e) {
            e.printStackTrace();
            return null;
        }
    }

}
