package im.redpanda.core;

import im.redpanda.core.exceptions.PeerProtocolException;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.Utils;
import org.junit.Test;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PeerTest {

    public static final int IVbytelen = 16;
    public static final String ALGORITHM = "AES/CTR/NoPadding";
    public static final String PROVIDER = "SunJCE";

    static {
        ByteBufferPool.init();
    }

    @Test
    public void getNodeId() {
    }

    @Test
    public void equalsIpAndPort() {

        Peer peer = new Peer("1.1.1.1", 123);
        Peer peer2 = new Peer("1.1.1.1", 123);
        Peer peer3 = new Peer("1.1.1.2", 123);
        Peer peer4 = new Peer("1.1.1.1", 124);

        assertTrue(peer.equalsIpAndPort(peer2));
        assertFalse(peer.equalsIpAndPort(peer3));
        assertFalse(peer.equalsIpAndPort(peer4));
    }

    @Test
    public void equalsNonce() {

        Peer peer = new Peer("1.1.1.1", 123);
        Peer peer2 = new Peer("1.1.1.1", 123);
        Peer peer3 = new Peer("1.1.1.1", 123);

        KademliaId id1 = new KademliaId();
        KademliaId id2 = new KademliaId();

        assertNotNull(peer);
        assertNotNull(id1);

        peer.setNodeId(new NodeId(id1));
        peer2.setNodeId(new NodeId(id1));
        peer3.setNodeId(new NodeId(id2));

        assertTrue(peer.equalsNonce(peer2));
        assertFalse(peer.equalsNonce(peer3));

    }

    @Test
    public void equalsInstance() {

        Peer peer = new Peer("1.1.1.1", 123);
        Peer peer2 = new Peer("1.1.1.1", 123);

        assertTrue(peer.equalsInstance(peer));
        assertFalse(peer.equalsInstance(peer2));

    }

    @Test
    public void setNodeId() {
        Peer peer = new Peer("1.1.1.1", 123);
        KademliaId id1 = new KademliaId();
        peer.setNodeId(new NodeId(id1));
        assertTrue(peer.getKademliaId().equals(id1));
    }

    @Test
    public void peerIsHigher() {
        ServerContext serverContext = ServerContext.buildDefaultServerContext();

        Peer peer = new Peer("1.1.1.1", 123);
        Peer peer2 = new Peer("1.1.1.1", 123);

        KademliaId kademliaId = KademliaId.fromFirstBytes(
                Utils.parseAsHexOrBase58("000000000000000000000000000000000000000000000000000000000000000000000000"));
        KademliaId kademliaId2 = KademliaId.fromFirstBytes(
                Utils.parseAsHexOrBase58("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"));

        byte[] bytes = Utils
                .parseAsHexOrBase58("000000000000000000000000000000000000000000000000000000000000000000000000");

        peer.setNodeId(new NodeId(kademliaId));
        peer2.setNodeId(new NodeId(kademliaId2));

        assertFalse(peer.peerIsHigher(serverContext));
        assertTrue(peer2.peerIsHigher(serverContext));

    }

    @Test
    public void decryptInputDataNoBytes() throws PeerProtocolException, InvalidAlgorithmParameterException,
            NoSuchPaddingException, NoSuchAlgorithmException, NoSuchProviderException, InvalidKeyException {
        Peer peer = new Peer("ip", 59558, new NodeId());

        setUpTestCipherStreams(peer);

        peer.readBuffer = ByteBuffer.allocate(80);

        ByteBuffer bufferIn = ByteBuffer.allocate(60);
        ByteBuffer bufferOut = ByteBuffer.allocate(60);
        bufferIn.flip();
        peer.getPeerChiperStreams().encrypt(bufferIn, bufferOut);

        int decryptedBytes = peer.decryptInputData(bufferOut);
        assertThat(decryptedBytes).isZero();
    }

    @Test
    public void decryptInputDataSimpleBytes() throws PeerProtocolException, NoSuchPaddingException,
            NoSuchAlgorithmException, NoSuchProviderException, InvalidAlgorithmParameterException, InvalidKeyException {
        Peer peer = new Peer("ip", 59558, new NodeId());

        setUpTestCipherStreams(peer);

        peer.readBuffer = ByteBuffer.allocate(80);

        long longToTest = new Random().nextLong();

        ByteBuffer bufferIn = ByteBuffer.allocate(60);
        bufferIn.putLong(longToTest);
        ByteBuffer bufferOut = ByteBuffer.allocate(60);
        bufferIn.flip();
        peer.getPeerChiperStreams().encrypt(bufferIn, bufferOut);

        int decryptedBytes = peer.decryptInputData(bufferOut);
        peer.readBuffer.flip();
        assertThat(decryptedBytes).isEqualTo(8);
        assertThat(peer.readBuffer.getLong()).isEqualTo(longToTest);
    }

    @Test
    public void decryptInputDataTooSmallReadBuffer() throws PeerProtocolException, NoSuchPaddingException,
            NoSuchAlgorithmException, NoSuchProviderException, InvalidAlgorithmParameterException, InvalidKeyException {
        Peer peer = new Peer("ip", 59558, new NodeId());

        setUpTestCipherStreams(peer);

        peer.readBuffer = ByteBufferPool.borrowObject(16);
        assertThat(peer.readBuffer.remaining()).isEqualTo(16);

        long longToTest = new Random().nextLong();

        ByteBuffer bufferIn = ByteBuffer.allocate(60);
        bufferIn.putLong(longToTest);
        bufferIn.putLong(longToTest);
        bufferIn.putLong(longToTest);
        ByteBuffer bufferOut = ByteBuffer.allocate(60);
        bufferIn.flip();
        peer.getPeerChiperStreams().encrypt(bufferIn, bufferOut);

        int decryptedBytes = peer.decryptInputData(bufferOut);
        peer.readBuffer.flip();
        assertThat(decryptedBytes).isEqualTo(24);
        assertThat(peer.readBuffer.getLong()).isEqualTo(longToTest);
        assertThat(peer.readBuffer.getLong()).isEqualTo(longToTest);
        assertThat(peer.readBuffer.getLong()).isEqualTo(longToTest);
    }

    private void setUpTestCipherStreams(Peer peer) throws NoSuchAlgorithmException, NoSuchProviderException,
            NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException {
        IvParameterSpec zeroIv = new IvParameterSpec(ByteBuffer.allocate(IVbytelen).array());

        ByteBuffer bytesForPrivateAESkeySend = ByteBuffer.allocate(32 + PeerInHandshake.IVbytelen);
        ByteBuffer bytesForPrivateAESkeyReceive = ByteBuffer.allocate(32 + PeerInHandshake.IVbytelen);

        Sha256Hash sha256HashSend = Sha256Hash.create(bytesForPrivateAESkeySend.array());
        Sha256Hash sha256HashReceive = Sha256Hash.create(bytesForPrivateAESkeyReceive.array());

        SecretKeySpec sharedSecretSend = new SecretKeySpec(sha256HashSend.getBytes(), "AES");
        SecretKeySpec sharedSecretReceive = new SecretKeySpec(sha256HashReceive.getBytes(), "AES");
        PeerOutputStream peerOutputStream = new PeerOutputStream();
        Cipher cipherSend = Cipher.getInstance(ALGORITHM, PROVIDER);
        cipherSend.init(Cipher.ENCRYPT_MODE, sharedSecretSend, zeroIv);
        CipherOutputStreamByteBuffer cipherOutputStream = new CipherOutputStreamByteBuffer(peerOutputStream,
                cipherSend);

        // lets set up the receive Cipher
        PeerInputStream peerInputStream = new PeerInputStream();
        Cipher cipherReceive = Cipher.getInstance(ALGORITHM, PROVIDER);
        cipherReceive.init(Cipher.DECRYPT_MODE, sharedSecretReceive, zeroIv);
        CipherInputStreamByteBuffer cipherInputStream = new CipherInputStreamByteBuffer(peerInputStream, cipherReceive);

        peer.setPeerChiperStreams(
                new PeerChiperStreams(peerOutputStream, peerInputStream, cipherInputStream, cipherOutputStream));
    }
}
