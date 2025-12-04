package im.redpanda.crypt;

import org.junit.Test;

import static org.junit.Assert.*;

public class Base58AdditionalTest {

    @Test
    public void testDecodeCheckedRoundtrip() throws Exception {
        byte[] data = new byte[]{'a', 'b', 'c'};
        byte[] checksumFull = Utils.doubleDigest(data);
        byte[] combined = new byte[data.length + 4];
        System.arraycopy(data, 0, combined, 0, data.length);
        System.arraycopy(checksumFull, 0, combined, data.length, 4);

        String encoded = Base58.encode(combined);
        byte[] decoded = Base58.decodeChecked(encoded);
        assertArrayEquals(data, decoded);
    }

    @Test
    public void testDecodeCheckedInvalidChecksumThrows() throws Exception {
        byte[] data = new byte[]{1, 2, 3, 4};
        byte[] checksumFull = Utils.doubleDigest(data);
        byte[] combined = new byte[data.length + 4];
        System.arraycopy(data, 0, combined, 0, data.length);
        System.arraycopy(checksumFull, 0, combined, data.length, 4);
        // Corrupt one checksum byte
        combined[combined.length - 1] ^= 0x01;

        String encoded = Base58.encode(combined);

        try {
            Base58.decodeChecked(encoded);
            fail("Expected AddressFormatException due to invalid checksum");
        } catch (AddressFormatException expected) {
            // ok
        }
    }
}
