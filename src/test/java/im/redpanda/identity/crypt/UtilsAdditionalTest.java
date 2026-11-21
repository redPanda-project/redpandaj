package im.redpanda.identity.crypt;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class UtilsAdditionalTest {

  @Test
  void doubleDigestKnownVector() {
    byte[] input = "hello".getBytes();
    byte[] dd = Utils.doubleDigest(input);
    String hex = Utils.bytesToHexString(dd);
    assertEquals("9595c9df90075148eb06860365df33584b75bff782a510c6cd4883a419833d50", hex);
  }

  @Test
  void doubleDigestOfARangeIgnoresTheBytesAroundIt() {
    byte[] input = "xxhelloyy".getBytes(StandardCharsets.US_ASCII);
    String expected = "9595c9df90075148eb06860365df33584b75bff782a510c6cd4883a419833d50";
    assertEquals(expected, Utils.bytesToHexString(Utils.doubleDigest(input, 2, 5)));
    // No state carries over from one call to the next.
    assertEquals(expected, Utils.bytesToHexString(Utils.doubleDigest(input, 2, 5)));
  }

  @Test
  void bytesToHexString() {
    byte[] arr = new byte[] {0x00, 0x0f, (byte) 0xff};
    assertEquals("000fff", Utils.bytesToHexString(arr));
  }
}
