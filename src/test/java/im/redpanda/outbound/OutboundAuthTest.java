package im.redpanda.outbound;

import static org.junit.Assert.assertEquals;

import im.redpanda.core.NodeId;
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
    return signV2(clientNode, payload);
  }

  private static byte[] signV2(NodeId node, byte[] payload) {
    byte[] versioned = new byte[payload.length + 1];
    versioned[0] = OutboundAuth.SIGNING_VERSION_ED25519;
    System.arraycopy(payload, 0, versioned, 1, payload.length);
    return node.sign(versioned);
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
   * sdd02 phase 2: the pre-MS03 brainpool ECDSA fallback is gone — a 65-byte public key (the
   * retired legacy format) is rejected as an unsupported key length, whatever the signature.
   */
  @Test
  public void testVerify_LegacyEcdsa65ByteKeyRejected() {
    long timestamp = System.currentTimeMillis();
    byte[] payload = "testPayload".getBytes();
    byte[] nonce = "nonceLegacy".getBytes();
    byte[] ohId = "legacyOhId".getBytes();

    byte[] legacy65ByteKey = new byte[65];
    legacy65ByteKey[0] = 0x04; // uncompressed-point marker of the old export format
    byte[] someSignature = new byte[70];

    OutboundAuth.AuthResult result =
        auth.verify(legacy65ByteKey, payload, someSignature, timestamp, ohId, nonce);

    assertEquals(OutboundAuth.AuthResult.INVALID_SIGNATURE, result);
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
  public void replay_sameCommandTwiceWithinWindow_secondRejected() {
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

  /**
   * sdd02 Phase 3: flooding from one source hits the per-source cap (further commands rejected,
   * never evicted) and does not displace other sources' replay-cache entries. The signature covers
   * only the payload here, so one signature per source is reused across unique nonces — nonce
   * uniqueness is what the cache tracks.
   */
  @Test
  public void floodFromOneSource_doesNotEvictOtherSources_andCapsSource() {
    long timestamp = System.currentTimeMillis();
    byte[] payload = "testPayload".getBytes();

    // Source B first: one accepted command that must survive the flood
    byte[] ohIdB = clientNode.getKademliaId().getBytes();
    byte[] nonceB = "nonceB".getBytes();
    byte[] signatureB = signV2(payload);
    assertEquals(
        OutboundAuth.AuthResult.OK,
        auth.verify(clientNode.getVerifyKeyBytes(), payload, signatureB, timestamp, ohIdB, nonceB));

    // Source A floods up to the cap — every command accepted
    NodeId attacker = NodeId.generateWithSimpleKey();
    byte[] ohIdA = attacker.getKademliaId().getBytes();
    byte[] signatureA = signV2(attacker, payload);
    for (int i = 0; i < OutboundAuth.MAX_ENTRIES_PER_SOURCE; i++) {
      assertEquals(
          "flood command " + i + " must be accepted",
          OutboundAuth.AuthResult.OK,
          auth.verify(
              attacker.getVerifyKeyBytes(),
              payload,
              signatureA,
              timestamp,
              ohIdA,
              ("floodNonce" + i).getBytes()));
    }

    // Command MAX+1 from A -> rejected by the cap
    assertEquals(
        OutboundAuth.AuthResult.REPLAY,
        auth.verify(
            attacker.getVerifyKeyBytes(),
            payload,
            signatureA,
            timestamp,
            ohIdA,
            "floodNonceOverCap".getBytes()));

    // Source B was not evicted: replaying its command is still detected
    assertEquals(
        OutboundAuth.AuthResult.REPLAY,
        auth.verify(clientNode.getVerifyKeyBytes(), payload, signatureB, timestamp, ohIdB, nonceB));
  }

  /**
   * sdd02 Phase 3: {@link OutboundAuth#cleanupCache(long)} removes only expired entries and
   * rebuilds the per-source counters, so a previously capped source can send again once its entries
   * have expired.
   */
  @Test
  public void cleanup_removesOnlyExpiredEntries_andRebuildsSourceCounts() {
    long now = System.currentTimeMillis();
    // Still inside the ±window when inserted (60 s margin so slow CI runs stay valid), but
    // expired after the cleanup below
    long agingTimestamp = now - OutboundAuth.TIME_WINDOW_MS + 60_000;
    byte[] payload = "testPayload".getBytes();

    // Cap source A with aging entries
    NodeId sourceA = NodeId.generateWithSimpleKey();
    byte[] ohIdA = sourceA.getKademliaId().getBytes();
    byte[] signatureA = signV2(sourceA, payload);
    for (int i = 0; i < OutboundAuth.MAX_ENTRIES_PER_SOURCE; i++) {
      assertEquals(
          OutboundAuth.AuthResult.OK,
          auth.verify(
              sourceA.getVerifyKeyBytes(),
              payload,
              signatureA,
              agingTimestamp,
              ohIdA,
              ("agingNonce" + i).getBytes()));
    }
    assertEquals(
        "source A must be capped before cleanup",
        OutboundAuth.AuthResult.REPLAY,
        auth.verify(
            sourceA.getVerifyKeyBytes(),
            payload,
            signatureA,
            agingTimestamp,
            ohIdA,
            "cappedNonce".getBytes()));

    // One fresh entry from source B that must survive the cleanup
    byte[] ohIdB = clientNode.getKademliaId().getBytes();
    byte[] nonceB = "freshNonceB".getBytes();
    byte[] signatureB = signV2(payload);
    assertEquals(
        OutboundAuth.AuthResult.OK,
        auth.verify(clientNode.getVerifyKeyBytes(), payload, signatureB, now, ohIdB, nonceB));

    // 130 s later the aging entries are outside the window, the fresh one is not
    auth.cleanupCache(now + 130_000);

    // Expired entries are gone: the same aging command is accepted again ...
    assertEquals(
        OutboundAuth.AuthResult.OK,
        auth.verify(
            sourceA.getVerifyKeyBytes(),
            payload,
            signatureA,
            agingTimestamp,
            ohIdA,
            "agingNonce0".getBytes()));

    // ... the fresh entry survived: replaying it is still detected ...
    assertEquals(
        OutboundAuth.AuthResult.REPLAY,
        auth.verify(clientNode.getVerifyKeyBytes(), payload, signatureB, now, ohIdB, nonceB));

    // ... and the rebuilt counters let the previously capped source send new commands
    assertEquals(
        OutboundAuth.AuthResult.OK,
        auth.verify(
            sourceA.getVerifyKeyBytes(),
            payload,
            signatureA,
            System.currentTimeMillis(),
            ohIdA,
            "newNonceAfterCleanup".getBytes()));
  }
}
