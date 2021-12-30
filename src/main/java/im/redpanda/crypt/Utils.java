package im.redpanda.crypt;

import org.bouncycastle.util.encoders.Hex;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;

public class Utils {

    private static final MessageDigest digest;

    static {
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }


    private Utils() {
    }


    /**
     * See {@link Utils#doubleDigest(byte[], int, int)}.
     */
    public static byte[] doubleDigest(byte[] input) {
        return doubleDigest(input, 0, input.length);
    }

    /**
     * Calculates the SHA-256 hash of the given byte range, and then hashes the
     * resulting hash again. This is standard procedure in Bitcoin. The
     * resulting hash is in big endian form.
     */
    public static byte[] doubleDigest(byte[] input, int offset, int length) {
        synchronized (digest) {
            digest.reset();
            digest.update(input, offset, length);
            byte[] first = digest.digest();
            return digest.digest(first);
        }
    }

    /**
     * Returns the given byte array hex encoded.
     */
    public static String bytesToHexString(byte[] bytes) {
        StringBuffer buf = new StringBuffer(bytes.length * 2);
        for (byte b : bytes) {
            String s = Integer.toString(0xFF & b, 16);
            if (s.length() < 2) {
                buf.append('0');
            }
            buf.append(s);
        }
        return buf.toString();
    }

    /**
     * Attempts to parse the given string as arbitrary-length hex or base58 and
     * then return the results, or null if neither parse was successful.
     */
    public static byte[] parseAsHexOrBase58(String data) {
        try {
            return Hex.decode(data);
        } catch (Exception e) {
            // Didn't decode as hex, try base58.
            try {
                return Base58.decodeChecked(data);
            } catch (AddressFormatException e1) {
                return null;
            }
        }
    }

    public static String formatDuration(long millis) {
        return formatDuration(Duration.ofMillis(millis));
    }

    public static String formatDurationFromNow(long millis) {
        return formatDuration(Duration.ofMillis(System.currentTimeMillis() - millis));
    }

    public static String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        long absSeconds = Math.abs(seconds);
        String positive = String.format(
                "%d:%02d:%02d",
                absSeconds / 3600,
                (absSeconds % 3600) / 60,
                absSeconds % 60);
        return seconds < 0 ? "-" + positive : positive;
    }

    public static byte[] readSignature(ByteBuffer readBuffer) {
        //second byte of encoding gives the remaining bytes of the signature, cf. eg. https://crypto.stackexchange.com/questions/1795/how-can-i-convert-a-der-ecdsa-signature-to-asn-1
        readBuffer.get();
        int lenOfSignature = ((int) readBuffer.get()) + 2;
        readBuffer.position(readBuffer.position() - 2);

        byte[] signature = new byte[lenOfSignature];
        if (readBuffer.remaining() < lenOfSignature) {
            return null;
        }
        readBuffer.get(signature);
        return signature;
    }

    public static boolean isLocalAddress(String string) {
        return string.equals("127.0.0.1") || string.equals("localhost") || string.startsWith("192.");
    }
}
