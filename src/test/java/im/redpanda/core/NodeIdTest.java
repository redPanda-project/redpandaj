package im.redpanda.core;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import im.redpanda.crypt.Utils;
import java.security.Security;
import org.junit.Test;

public class NodeIdTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void exportWithPrivate() {

    NodeId nodeId = new NodeId();

    byte[] bytes = nodeId.exportWithPrivate();
    assertTrue(bytes.length == NodeId.PRIVATE_KEYLEN);
  }

  @Test
  public void importWithPrivate() {

    NodeId nodeId = new NodeId();

    byte[] bytes = nodeId.exportWithPrivate();

    NodeId nodeId1 = NodeId.importWithPrivate(bytes);

    assertNotNull(nodeId1);

    assertTrue(nodeId1.equals(nodeId));
  }

  @Test
  public void exportPublic() {

    NodeId nodeId = new NodeId();

    byte[] bytes = nodeId.exportPublic();

    assertTrue(bytes.length == NodeId.PUBLIC_KEYLEN);
  }

  @Test
  public void importPublic() {

    NodeId nodeId = new NodeId();

    byte[] bytes = nodeId.exportPublic();

    NodeId nodeId1 = NodeId.importPublic(bytes);

    assertTrue(nodeId1.equals(nodeId));
  }

  @Test
  public void testSignatures() {
    for (int i = 0; i < 1; i++) {

      byte[] bytes = "Test Message".getBytes();

      NodeId nodeId = new NodeId();
      byte[] signature = nodeId.sign(bytes);

      System.out.println("messagebytes: " + Utils.bytesToHexString(bytes));
      System.out.println("pubkey: " + Utils.bytesToHexString(nodeId.exportPublic()));
      System.out.println("signature: " + Utils.bytesToHexString(signature));
      System.out.println("");

      assertTrue(nodeId.verify(bytes, signature));
    }
  }

  @Test
  public void signatureIsFixed64Bytes() {
    NodeId nodeId = new NodeId();
    byte[] signature = nodeId.sign("MS03".getBytes());
    assertTrue(signature.length == NodeId.SIGNATURE_LEN);
  }

  @Test
  public void verifyRejectsWrongKeyAndTamperedData() {
    byte[] bytes = "Test Message".getBytes();
    NodeId nodeId = new NodeId();
    NodeId otherNodeId = new NodeId();
    byte[] signature = nodeId.sign(bytes);

    assertTrue(!otherNodeId.verify(bytes, signature));
    assertTrue(!nodeId.verify("Test Message!".getBytes(), signature));
    assertTrue(!nodeId.verify(bytes, new byte[NodeId.SIGNATURE_LEN]));
    assertTrue(!nodeId.verify(bytes, new byte[12]));
  }

  @Test
  public void fromSeedIsDeterministicAndSeparatesKeys() {
    byte[] seed = new byte[32];
    seed[0] = 42;

    NodeId first = NodeId.fromSeed(seed);
    NodeId second = NodeId.fromSeed(seed);

    assertTrue(java.util.Arrays.equals(first.exportPublic(), second.exportPublic()));
    assertTrue(first.equals(second));

    // different seed -> different identity
    seed[0] = 43;
    NodeId third = NodeId.fromSeed(seed);
    assertTrue(!java.util.Arrays.equals(first.exportPublic(), third.exportPublic()));

    // signing key (seed) and encryption key (SHA-256 of seed) must differ
    byte[] privateExport = first.exportWithPrivate();
    byte[] signingKey = java.util.Arrays.copyOfRange(privateExport, 0, 32);
    byte[] encryptionKey = java.util.Arrays.copyOfRange(privateExport, 64, 96);
    assertTrue(!java.util.Arrays.equals(signingKey, encryptionKey));
  }

  @Test
  public void kademliaIdIsSha256OfVerifyKey() {
    NodeId nodeId = new NodeId();
    byte[] verifyKey = nodeId.getVerifyKeyBytes();
    KademliaId expected =
        KademliaId.fromFirstBytes(im.redpanda.crypt.Sha256Hash.create(verifyKey).getBytes());
    assertTrue(nodeId.getKademliaId().equals(expected));
  }

  @Test
  public void checkValidEnforcesLeadingZeroBitsOfDoubleSha256() {
    // generate until we find one valid and one invalid identity (PoW skipped only in ctor loop)
    boolean foundValid = false;
    boolean foundInvalid = false;
    for (int i = 0; i < 10_000 && !(foundValid && foundInvalid); i++) {
      NodeId nodeId = NodeId.generateWithSimpleKey();
      byte[] doubleHash =
          im.redpanda.crypt.Sha256Hash.createDouble(nodeId.getVerifyKeyBytes()).getBytes();
      boolean expected =
          im.redpanda.crypt.CryptoUtils.countLeadingZeroBits(doubleHash)
              >= NodeId.POW_MIN_LEADING_ZERO_BITS;
      assertTrue(nodeId.checkValid() == expected);
      foundValid |= expected;
      foundInvalid |= !expected;
    }
    assertTrue(foundValid && foundInvalid);
  }

  @Test
  public void importPublicRejectsLegacyLength() {
    try {
      NodeId.importPublic(new byte[65]);
      assertTrue("expected IllegalArgumentException", false);
    } catch (IllegalArgumentException expected) {
      // pre-MS03 brainpool exports (65 bytes) are not valid NodeIds anymore
    }
  }
}
