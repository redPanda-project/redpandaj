package im.redpanda.core;

import org.junit.Test;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CipherStreamsEdgeCaseTest {

    private static final String ALGO = "AES/CTR/NoPadding";
    private static final String PROVIDER = "SunJCE";

    @Test
    public void cipherOutputStream_writeRespectsLenWithNonZeroPosition() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException, IOException, java.security.NoSuchProviderException {
        SecretKeySpec key = new SecretKeySpec(new byte[16], "AES");
        IvParameterSpec iv = new IvParameterSpec(new byte[16]);

        Cipher enc = Cipher.getInstance(ALGO, PROVIDER);
        enc.init(Cipher.ENCRYPT_MODE, key, iv);

        PeerOutputStream target = new PeerOutputStream();
        ByteBuffer outBuf = ByteBuffer.allocate(64);
        target.setByteBuffer(outBuf);

        CipherOutputStreamByteBuffer cos = new CipherOutputStreamByteBuffer(target, enc);

        byte[] src = new byte[]{0,1,2,3,4,5,6,7,8,9};
        ByteBuffer inBuf = ByteBuffer.wrap(src);
        // Simulate partially consumed input
        inBuf.position(6); // remaining = 4

        // Write only 2 bytes from current position
        cos.write(inBuf, 2);
        cos.flush();

        // Only 2 bytes should have been written downstream
        assertThat(outBuf.position(), equalTo(2));
        // And input buffer position advanced by 2
        assertThat(inBuf.position(), equalTo(8));
    }

    @Test
    public void cipherInputStream_readRespectsRemainingWithNonZeroPosition() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException, IOException, java.security.NoSuchProviderException {
        SecretKeySpec key = new SecretKeySpec(new byte[16], "AES");
        IvParameterSpec iv = new IvParameterSpec(new byte[16]);

        // Prepare ciphertext by encrypting 10 bytes
        Cipher enc = Cipher.getInstance(ALGO, PROVIDER);
        enc.init(Cipher.ENCRYPT_MODE, key, iv);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CipherOutputStreamByteBuffer cos = new CipherOutputStreamByteBuffer(baos, enc);
        byte[] plain = new byte[10];
        ByteBuffer plainBuf = ByteBuffer.wrap(plain);
        cos.write(plainBuf);
        cos.flush();
        byte[] ciphertext = baos.toByteArray();

        // Set up decrypting stream reading from ciphertext
        PeerInputStream pis = new PeerInputStream();
        pis.setByteBuffer(ByteBuffer.wrap(ciphertext));

        Cipher dec = Cipher.getInstance(ALGO, PROVIDER);
        dec.init(Cipher.DECRYPT_MODE, key, iv);
        CipherInputStreamByteBuffer cis = new CipherInputStreamByteBuffer(pis, dec);

        // Output buffer with non-zero position and tight limit
        ByteBuffer out = ByteBuffer.allocate(10);
        out.position(3);
        out.limit(7); // remaining = 4

        // Read into the buffer; should only fill remaining bytes (4)
        cis.read(out);

        assertThat(out.position(), equalTo(7));
    }
}
