package im.redpanda.crypt;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class UtilsIsLocalAddressTest {

  @Test
  public void testLocalAndNonLocalAddresses() {
    assertTrue(im.redpanda.crypt.Utils.isLocalAddress("127.0.0.1"));
    assertTrue(im.redpanda.crypt.Utils.isLocalAddress("localhost"));
    assertTrue(im.redpanda.crypt.Utils.isLocalAddress("192.168.1.10"));
    assertFalse(im.redpanda.crypt.Utils.isLocalAddress("10.0.0.1"));
    assertFalse(im.redpanda.crypt.Utils.isLocalAddress("example.com"));
  }
}
