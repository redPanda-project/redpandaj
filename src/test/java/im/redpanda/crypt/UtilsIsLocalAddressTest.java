package im.redpanda.crypt;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class UtilsIsLocalAddressTest {

  @Test
  public void testLocalAndNonLocalAddresses() {
    assertTrue(Utils.isLocalAddress("127.0.0.1"));
    assertTrue(Utils.isLocalAddress("localhost"));
    assertTrue(Utils.isLocalAddress("192.168.1.10"));
    // 10/8 is RFC1918 and used to be reported as non-local
    assertTrue(Utils.isLocalAddress("10.0.0.1"));
    assertFalse(Utils.isLocalAddress("example.com"));
  }

  @Test
  public void coversTheWholeLoopbackAndPrivateSpace() {
    assertTrue(Utils.isLocalAddress("127.0.0.1"));
    assertTrue(Utils.isLocalAddress("127.255.255.254"));
    assertTrue(Utils.isLocalAddress("10.0.2.2")); // emulator gate host address
    assertTrue(Utils.isLocalAddress("172.16.0.1"));
    assertTrue(Utils.isLocalAddress("172.31.255.255"));
    assertTrue(Utils.isLocalAddress("192.168.0.1"));
    assertTrue(Utils.isLocalAddress("169.254.10.1"));
    assertTrue(Utils.isLocalAddress("100.64.0.1")); // CGNAT
    assertTrue(Utils.isLocalAddress("0.0.0.0"));
    assertTrue(Utils.isLocalAddress("::1"));
    assertTrue(Utils.isLocalAddress("0:0:0:0:0:0:0:1"));
    assertTrue(Utils.isLocalAddress("[::1]"));
    assertTrue(Utils.isLocalAddress("::"));
    assertTrue(Utils.isLocalAddress("fd00::1"));
    assertTrue(Utils.isLocalAddress("fe80::1%eth0"));
    assertTrue(Utils.isLocalAddress("::ffff:127.0.0.1"));
    assertTrue(Utils.isLocalAddress(null));
    assertTrue(Utils.isLocalAddress("  "));
  }

  @Test
  public void publicAddressesAreNotLocal() {
    assertFalse(Utils.isLocalAddress("5.75.137.166"));
    assertFalse(Utils.isLocalAddress("195.201.25.223"));
    assertFalse(Utils.isLocalAddress("84.147.60.253"));
    // only 192.168/16 is private, the rest of 192/8 is public - this was over-matched before
    assertFalse(Utils.isLocalAddress("192.0.2.5"));
    assertFalse(Utils.isLocalAddress("172.32.0.1"));
    assertFalse(Utils.isLocalAddress("100.128.0.1"));
    assertFalse(Utils.isLocalAddress("2a01:4f8::1"));
    assertFalse(Utils.isLocalAddress("redpanda.im"));
  }

  @Test
  public void publicAdvertisementIsAlwaysPlausible() {
    assertTrue(Utils.isPlausibleAdvertisedAddress("5.75.137.166", 59558, "46.224.156.238"));
    assertTrue(Utils.isPlausibleAdvertisedAddress("5.75.137.166", 59558, "127.0.0.1"));
  }

  @Test
  public void localAdvertisementOnlyPlausibleFromLocalPeer() {
    // a peer we reached over loopback may gossip loopback and LAN addresses...
    assertTrue(Utils.isPlausibleAdvertisedAddress("127.0.0.1", 59560, "127.0.0.1"));
    assertTrue(Utils.isPlausibleAdvertisedAddress("192.168.1.5", 59558, "10.0.2.2"));
    // ...a peer we reached over a public address may not
    assertFalse(Utils.isPlausibleAdvertisedAddress("127.0.0.1", 59558, "84.147.60.253"));
    assertFalse(Utils.isPlausibleAdvertisedAddress("10.0.0.9", 59558, "84.147.60.253"));
    assertFalse(Utils.isPlausibleAdvertisedAddress("192.168.1.5", 59558, "84.147.60.253"));
    assertFalse(Utils.isPlausibleAdvertisedAddress("172.20.1.1", 59558, "84.147.60.253"));
    // an unknown peer address is treated as not local
    assertFalse(Utils.isPlausibleAdvertisedAddress("127.0.0.1", 59558, null));
  }

  /**
   * A gossiped host name is never plausible: {@code isLocalAddress} classifies by string and cannot
   * see through a name, while {@code OutboundHandler} dials via {@code InetSocketAddress}, which
   * resolves it — so a name an attacker controls could resolve (or later be re-pointed) to loopback
   * or a LAN address and bypass the locality rule entirely.
   */
  @Test
  public void gossipedHostNamesAreNeverPlausible() {
    assertFalse(Utils.isPlausibleAdvertisedAddress("redpanda.im", 59559, "84.147.60.253"));
    assertFalse(Utils.isPlausibleAdvertisedAddress("evil.example.com", 59558, "84.147.60.253"));
    // also from a loopback peer - a name is untrusted regardless of who sends it
    assertFalse(Utils.isPlausibleAdvertisedAddress("redpanda.im", 59559, "127.0.0.1"));
    assertFalse(Utils.isPlausibleAdvertisedAddress("localhost", 59558, "127.0.0.1"));
    // a name that resolves to loopback today and to anything else tomorrow
    assertFalse(Utils.isPlausibleAdvertisedAddress("localtest.me", 59558, "127.0.0.1"));
    // literals stay plausible, including the loopback ones the e2e topology exchanges
    assertTrue(Utils.isPlausibleAdvertisedAddress("127.0.0.1", 59560, "127.0.0.1"));
    assertTrue(Utils.isPlausibleAdvertisedAddress("2a01:4f8::1", 59558, "84.147.60.253"));
  }

  @Test
  public void ipLiteralsAreDistinguishedFromNames() {
    assertTrue(Utils.isIpLiteral("127.0.0.1"));
    assertTrue(Utils.isIpLiteral("5.75.137.166"));
    assertTrue(Utils.isIpLiteral("::1"));
    assertTrue(Utils.isIpLiteral("[2a01:4f8::1]"));
    assertTrue(Utils.isIpLiteral("fe80::1%eth0"));
    assertTrue(Utils.isIpLiteral("::ffff:127.0.0.1"));
    assertFalse(Utils.isIpLiteral("redpanda.im"));
    assertFalse(Utils.isIpLiteral("localhost"));
    assertFalse(Utils.isIpLiteral("127.0.0.1.example.com"));
    assertFalse(Utils.isIpLiteral("999.1.1.1"));
    assertFalse(Utils.isIpLiteral(""));
    assertFalse(Utils.isIpLiteral(null));
  }

  @Test
  public void unusableAdvertisementsAreRejectedRegardlessOfOrigin() {
    // port 0 is what an inbound-only peer carries; nobody can dial it
    assertFalse(Utils.isPlausibleAdvertisedAddress("84.147.60.253", 0, "127.0.0.1"));
    assertFalse(Utils.isPlausibleAdvertisedAddress("84.147.60.253", 70000, "127.0.0.1"));
    assertFalse(Utils.isPlausibleAdvertisedAddress("84.147.60.253", -1, "127.0.0.1"));
    assertFalse(Utils.isPlausibleAdvertisedAddress("", 59558, "127.0.0.1"));
    assertFalse(Utils.isPlausibleAdvertisedAddress(null, 59558, "127.0.0.1"));
    assertFalse(Utils.isPlausibleAdvertisedAddress("0.0.0.0", 59558, "127.0.0.1"));
    assertFalse(Utils.isPlausibleAdvertisedAddress("::", 59558, "127.0.0.1"));
  }

  @Test
  public void ownHostAddressRecognisesLoopback() {
    assertTrue(Utils.isOwnHostAddress("127.0.0.1"));
    assertTrue(Utils.isOwnHostAddress("localhost"));
    assertTrue(Utils.isOwnHostAddress("::1"));
    assertFalse(Utils.isOwnHostAddress("84.147.60.253"));
    assertFalse(Utils.isOwnHostAddress(null));
  }
}
