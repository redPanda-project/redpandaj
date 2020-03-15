package im.redpanda.core;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The OutputStreams are for encryption and the InputStreams for decryption.
 */
public class PeerChiperStreams {

    private Cipher cipherSend;
    private Cipher cipherReceive;

    private PeerOutputStream peerOutputStream;
    private PeerInputStream peerInputStream;

    private CipherInputStreamByteBuffer cipherInputStream;
    private CipherOutputStreamByteBuffer cipherOutputStream;

    public PeerChiperStreams(Cipher cipherSend, Cipher cipherReceive, PeerOutputStream peerOutputStream, PeerInputStream peerInputStream, CipherInputStreamByteBuffer cipherInputStream, CipherOutputStreamByteBuffer cipherOutputStream) {
        this.cipherSend = cipherSend;
        this.cipherReceive = cipherReceive;
        this.peerOutputStream = peerOutputStream;
        this.peerInputStream = peerInputStream;
        this.cipherInputStream = cipherInputStream;
        this.cipherOutputStream = cipherOutputStream;
    }

    /**
     * Input has to be in read mode, output has to be in default (write) mode.
     *
     * @param input
     * @param output
     */
    public void encrypt(ByteBuffer input, ByteBuffer output) {

        /**
         * encrypt so we have to use the outputstreams
         * The bytes will flow the following way: input -> cipherOutputStream -> peerOutputStream -> output
         */

        try {

            int r = output.remaining();

            if (r < input.remaining()) {
                System.out.println("enc write buffer too small...");
                peerOutputStream.setByteBuffer(output);
                cipherOutputStream.write(input, r);
                cipherOutputStream.flush();
            } else {
                peerOutputStream.setByteBuffer(output);
                cipherOutputStream.write(input);
                cipherOutputStream.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /**
     * Input has to be in read mode, output has to be in default (write) mode.
     *
     * @param input
     * @param output
     */
    public void decrypt(ByteBuffer input, ByteBuffer output) {

        /**
         * decrypt so we have to use the inputStreams
         * The bytes will flow the following way: input -> peerInputStream -> cipherInputStream -> output
         */

        try {
            peerInputStream.setByteBuffer(input);
            while (input.hasRemaining()) {
                cipherInputStream.read(output);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

//    public void encrypt(byte[] input, byte[] output) {
//        encrypt(ByteBuffer.wrap(input), ByteBuffer.wrap(output));
//    }
//
//    public void decrypt(byte[] input, byte[] output) {
//        decrypt(ByteBuffer.wrap(input), ByteBuffer.wrap(output));
//    }

}
