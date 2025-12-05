package im.redpanda.crypt;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class UtilsAdditionalTest {

    @Test
    public void testDoubleDigestKnownVector() {
        byte[] input = "hello".getBytes();
        byte[] dd = Utils.doubleDigest(input);
        String hex = Utils.bytesToHexString(dd);
        assertEquals("9595c9df90075148eb06860365df33584b75bff782a510c6cd4883a419833d50", hex);
    }

    @Test
    public void testBytesToHexString() {
        byte[] arr = new byte[]{0x00, 0x0f, (byte) 0xff};
        assertEquals("000fff", Utils.bytesToHexString(arr));
    }

    @Test
    public void testReadSignatureValidAndShortBuffer() {
        byte[] sig = new byte[]{
                0x30, 0x06, // DER header + length (6) => total length including header = 8
                0x02, 0x01, 0x01,
                0x02, 0x01, 0x02
        };

        ByteBuffer ok = ByteBuffer.wrap(sig);
        byte[] read = Utils.readSignature(ok);
        assertNotNull(read);
        assertEquals(8, read.length);
        assertEquals(0x30, read[0] & 0xff);
        assertEquals(0x06, read[1] & 0xff);

        // Now provide a too-short buffer (7 bytes, but header claims 8)
        byte[] shortSig = new byte[]{
                0x30, 0x06,
                0x02, 0x01, 0x01,
                0x02, 0x01
        };
        ByteBuffer shortBuf = ByteBuffer.wrap(shortSig);
        assertNull(Utils.readSignature(shortBuf));
    }
}
