package im.redpanda.crypt;

import static org.junit.Assert.*;

import org.junit.Test;

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
    byte[] arr = new byte[] {0x00, 0x0f, (byte) 0xff};
    assertEquals("000fff", Utils.bytesToHexString(arr));
  }
}
