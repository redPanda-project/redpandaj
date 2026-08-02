package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import im.redpanda.crypt.Utils;
import java.security.Security;
import org.junit.jupiter.api.Test;

class NodeIdTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  void exportWithPrivate() {

    NodeId nodeId = new NodeId();

    byte[] bytes = nodeId.exportWithPrivate();
    assertEquals(NodeId.PRIVATE_KEYLEN, bytes.length);
  }

  @Test
  void importWithPrivate() {

    NodeId nodeId = new NodeId();

    byte[] bytes = nodeId.exportWithPrivate();

    NodeId nodeId1 = NodeId.importWithPrivate(bytes);

    assertNotNull(nodeId1);

    assertEquals(nodeId1, nodeId);
  }

  @Test
  void exportPublic() {

    NodeId nodeId = new NodeId();

    byte[] bytes = nodeId.exportPublic();

    assertEquals(NodeId.PUBLIC_KEYLEN, bytes.length);
  }

  @Test
  void importPublic() {

    NodeId nodeId = new NodeId();

    byte[] bytes = nodeId.exportPublic();

    NodeId nodeId1 = NodeId.importPublic(bytes);

    assertEquals(nodeId1, nodeId);
  }

  @Test
  void signatures() {
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
  void signatureIsFixed64Bytes() {
    NodeId nodeId = new NodeId();
    byte[] signature = nodeId.sign("MS03".getBytes());
    assertEquals(NodeId.SIGNATURE_LEN, signature.length);
  }

  @Test
  void verifyRejectsWrongKeyAndTamperedData() {
    byte[] bytes = "Test Message".getBytes();
    NodeId nodeId = new NodeId();
    NodeId otherNodeId = new NodeId();
    byte[] signature = nodeId.sign(bytes);

    assertFalse(otherNodeId.verify(bytes, signature));
    assertFalse(nodeId.verify("Test Message!".getBytes(), signature));
    assertFalse(nodeId.verify(bytes, new byte[NodeId.SIGNATURE_LEN]));
    assertFalse(nodeId.verify(bytes, new byte[12]));
  }

  @Test
  void fromSeedIsDeterministicAndSeparatesKeys() {
    byte[] seed = new byte[32];
    seed[0] = 42;

    NodeId first = NodeId.fromSeed(seed);
    NodeId second = NodeId.fromSeed(seed);

    assertTrue(java.util.Arrays.equals(first.exportPublic(), second.exportPublic()));
    assertEquals(first, second);

    // different seed -> different identity
    seed[0] = 43;
    NodeId third = NodeId.fromSeed(seed);
    assertFalse(java.util.Arrays.equals(first.exportPublic(), third.exportPublic()));

    // signing key (seed) and encryption key (SHA-256 of seed) must differ
    byte[] privateExport = first.exportWithPrivate();
    byte[] signingKey = java.util.Arrays.copyOfRange(privateExport, 0, 32);
    byte[] encryptionKey = java.util.Arrays.copyOfRange(privateExport, 64, 96);
    assertFalse(java.util.Arrays.equals(signingKey, encryptionKey));
  }

  @Test
  void kademliaIdIsSha256OfVerifyKey() {
    NodeId nodeId = new NodeId();
    byte[] verifyKey = nodeId.getVerifyKeyBytes();
    KademliaId expected =
        KademliaId.fromFirstBytes(im.redpanda.crypt.Sha256Hash.create(verifyKey).getBytes());
    assertEquals(nodeId.getKademliaId(), expected);
  }

  @Test
  void checkValidEnforcesLeadingZeroBitsOfDoubleSha256() {
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
      assertEquals(nodeId.checkValid(), expected);
      foundValid |= expected;
      foundInvalid |= !expected;
    }
    assertTrue(foundValid && foundInvalid);
  }

  @Test
  void importPublicRejectsLegacyLength() {
    try {
      NodeId.importPublic(new byte[65]);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // pre-MS03 brainpool exports (65 bytes) are not valid NodeIds anymore
    }
  }
}
