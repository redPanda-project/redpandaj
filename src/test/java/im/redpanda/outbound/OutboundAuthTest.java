package im.redpanda.outbound;

import static org.junit.Assert.assertEquals;

import im.redpanda.core.NodeId;
import im.redpanda.crypt.legacy.LegacyNodeId;
import org.junit.Before;
import org.junit.Test;

public class OutboundAuthTest {

  private OutboundAuth auth;
  private NodeId clientNode;

  @Before
  public void setUp() {
    auth = new OutboundAuth();
    // Generate a valid keypair for testing
    clientNode = NodeId.generateWithSimpleKey();
  }

  /**
   * Signs the payload in the MS03 v2 signing-byte format: {@code [0x02 | payload]} — what a current
   * (Ed25519) client signs and what {@link OutboundAuth} verifies for 32-byte keys.
   */
  private byte[] signV2(byte[] payload) {
    byte[] versioned = new byte[payload.length + 1];
    versioned[0] = OutboundAuth.SIGNING_VERSION_ED25519;
    System.arraycopy(payload, 0, versioned, 1, payload.length);
    return clientNode.sign(versioned);
  }

  @Test
  public void testVerify_Valid() {
    long timestamp = System.currentTimeMillis();
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] payload = "testPayload".getBytes();
    byte[] nonce = "nonce1".getBytes();

    byte[] signature = signV2(payload);

    OutboundAuth.AuthResult result =
        auth.verify(clientNode.getVerifyKeyBytes(), payload, signature, timestamp, ohId, nonce);

    assertEquals(OutboundAuth.AuthResult.OK, result);
  }

  @Test
  public void testVerify_InvalidSignature() {
    long timestamp = System.currentTimeMillis();
    byte[] payload = "testPayload".getBytes();
    byte[] nonce = "nonce2".getBytes();
    byte[] ohId = clientNode.getKademliaId().getBytes();

    byte[] signature = signV2(payload);

    // Verify with DIFFERENT payload
    byte[] tamperedPayload = "tampered".getBytes();

    OutboundAuth.AuthResult result =
        auth.verify(
            clientNode.getVerifyKeyBytes(), tamperedPayload, signature, timestamp, ohId, nonce);

    assertEquals(OutboundAuth.AuthResult.INVALID_SIGNATURE, result);
  }

  /**
   * MS03 §8: an Ed25519 signature over the unversioned (pre-MS03) signing bytes must be rejected —
   * the version/algorithm byte is part of the signed data.
   */
  @Test
  public void testVerify_MissingSigningVersionByteIsRejected() {
    long timestamp = System.currentTimeMillis();
    byte[] payload = "testPayload".getBytes();
    byte[] nonce = "nonceNoVersion".getBytes();
    byte[] ohId = clientNode.getKademliaId().getBytes();

    byte[] unversionedSignature = clientNode.sign(payload);

    OutboundAuth.AuthResult result =
        auth.verify(
            clientNode.getVerifyKeyBytes(), payload, unversionedSignature, timestamp, ohId, nonce);

    assertEquals(OutboundAuth.AuthResult.INVALID_SIGNATURE, result);
  }

  /**
   * MS03 dual-version transition: legacy (pre-MS03) light clients sign the unversioned v1 bytes
   * with brainpool ECDSA and send their 65-byte public key — still accepted until protocol-v22
   * support is removed.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testVerify_LegacyEcdsaV1FormatStillAccepted() {
    LegacyNodeId legacyClient = LegacyNodeId.generate();

    long timestamp = System.currentTimeMillis();
    byte[] payload = "testPayload".getBytes();
    byte[] nonce = "nonceLegacy".getBytes();
    byte[] ohId = "legacyOhId".getBytes();

    // v1 signing bytes: no version byte
    byte[] signature = legacyClient.sign(payload);

    OutboundAuth.AuthResult result =
        auth.verify(legacyClient.exportPublic(), payload, signature, timestamp, ohId, nonce);

    assertEquals(OutboundAuth.AuthResult.OK, result);

    // tampered payload must still fail
    OutboundAuth.AuthResult tampered =
        auth.verify(
            legacyClient.exportPublic(),
            "tampered".getBytes(),
            signature,
            timestamp,
            ohId,
            "nonceLegacy2".getBytes());
    assertEquals(OutboundAuth.AuthResult.INVALID_SIGNATURE, tampered);
  }

  @Test
  public void testVerify_InvalidTimestamp() {
    long now = System.currentTimeMillis();
    long oldTimestamp = now - (10 * 60 * 1000); // 10 mins ago (window is 5 mins)

    byte[] payload = "testPayload".getBytes();
    byte[] nonce = "nonce3".getBytes();
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] signature = signV2(payload); // Sig is valid, but timestamp logic is separate check

    OutboundAuth.AuthResult result =
        auth.verify(clientNode.getVerifyKeyBytes(), payload, signature, oldTimestamp, ohId, nonce);

    assertEquals(OutboundAuth.AuthResult.INVALID_TIMESTAMP, result);
  }

  @Test
  public void testVerify_Replay() {
    long timestamp = System.currentTimeMillis();
    byte[] payload = "testPayload".getBytes();
    byte[] nonce = "nonceReplay".getBytes();
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] signature = signV2(payload);

    // First call -> OK
    OutboundAuth.AuthResult result1 =
        auth.verify(clientNode.getVerifyKeyBytes(), payload, signature, timestamp, ohId, nonce);
    assertEquals(OutboundAuth.AuthResult.OK, result1);

    // Second call same params -> REPLAY
    OutboundAuth.AuthResult result2 =
        auth.verify(clientNode.getVerifyKeyBytes(), payload, signature, timestamp, ohId, nonce);
    assertEquals(OutboundAuth.AuthResult.REPLAY, result2);
  }
}
