package im.redpanda.crypt.legacy;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import im.redpanda.outbound.OutboundAuth;
import java.security.Security;
import org.junit.Test;

/**
 * MS03 transition path: the deprecated brainpool identity used for protocol-v22 light clients must
 * keep working (65-byte public export, SHA256withECDSA signatures) until the legacy support is
 * removed.
 */
@SuppressWarnings("deprecation")
public class LegacyNodeIdTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  public void publicExportRoundtripKeeps65ByteFormatAndKademliaId() {
    LegacyNodeId legacyNodeId = LegacyNodeId.generate();

    byte[] exported = legacyNodeId.exportPublic();
    assertEquals(LegacyNodeId.PUBLIC_KEYLEN, exported.length);

    LegacyNodeId imported = LegacyNodeId.importPublic(exported);
    assertArrayEquals(exported, imported.exportPublic());
    assertEquals(legacyNodeId.getKademliaId(), imported.getKademliaId());
  }

  @Test
  public void legacyEcdsaSignaturesVerify() {
    LegacyNodeId legacyNodeId = LegacyNodeId.generate();
    byte[] message = "pre-MS03 light client".getBytes();

    byte[] signature = legacyNodeId.sign(message);

    LegacyNodeId verifier = LegacyNodeId.importPublic(legacyNodeId.exportPublic());
    assertTrue(verifier.verify(message, signature));
    assertFalse(verifier.verify("tampered".getBytes(), signature));
  }

  /**
   * OH auth requests from pre-MS03 light clients carry a 65-byte brainpool key and a DER ECDSA
   * signature — the dual-version OutboundAuth must still accept them during the transition.
   */
  @Test
  public void outboundAuthAcceptsLegacyEcdsaSignedRequest() {
    LegacyNodeId clientKey = LegacyNodeId.generate();
    byte[] signingBytes = "legacy signing bytes".getBytes();
    byte[] signature = clientKey.sign(signingBytes);

    OutboundAuth auth = new OutboundAuth();
    OutboundAuth.AuthResult result =
        auth.verify(
            clientKey.exportPublic(),
            signingBytes,
            signature,
            System.currentTimeMillis(),
            new byte[20],
            "nonce-legacy".getBytes());

    assertEquals(OutboundAuth.AuthResult.OK, result);
  }

  @Test
  public void outboundAuthRejectsUnsupportedKeyLength() {
    OutboundAuth auth = new OutboundAuth();
    OutboundAuth.AuthResult result =
        auth.verify(
            new byte[33],
            "bytes".getBytes(),
            new byte[64],
            System.currentTimeMillis(),
            new byte[20],
            "nonce-badkey".getBytes());

    assertEquals(OutboundAuth.AuthResult.INVALID_SIGNATURE, result);
  }
}
