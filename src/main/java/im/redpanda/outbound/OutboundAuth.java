package im.redpanda.outbound;

import im.redpanda.core.NodeId;
import im.redpanda.crypt.Utils;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OutboundAuth {

  private static final Logger logger = LogManager.getLogger();

  // Allow requests within ±5 minutes of server time
  // Allow requests within ±5 minutes of server time
  private static final long TIME_WINDOW_MS = 5L * 60L * 1000L;

  // Simple replay cache: (ohId + nonce) -> timestamp
  // In a real prod environment, this should be LRU or expire based on timestamp
  private final ConcurrentHashMap<String, Long> replayCache = new ConcurrentHashMap<>();

  public enum AuthResult {
    OK,
    INVALID_SIGNATURE,
    INVALID_TIMESTAMP,
    REPLAY
  }

  public AuthResult verify(
      byte[] ohAuthPublicKey,
      byte[] signingBytes,
      byte[] signature,
      long timestampMs,
      byte[] ohId,
      byte[] nonce) {

    // 1. Check Timestamp
    long now = System.currentTimeMillis();
    if (Math.abs(now - timestampMs) > TIME_WINDOW_MS) {
      logger.warn("OutboundAuth: Invalid timestamp. Client: {}, Server: {}", timestampMs, now);
      return AuthResult.INVALID_TIMESTAMP;
    }

    // 2. Check Replay
    String replayKey = Utils.bytesToHexString(ohId) + "_" + Utils.bytesToHexString(nonce);
    if (replayCache.containsKey(replayKey)) {
      // In a robust implementation, we'd check if the entry is old enough to be
      // forgotten,
      // but for now, if it's in the map, it's a replay (assuming map is cleaned up or
      // small enough
      // for PoC)
      logger.warn("OutboundAuth: Replay detected for key {}", replayKey);
      return AuthResult.REPLAY;
    }

    // 3. Verify Signature — dual-version support (MS03 transition):
    //    32-byte key  -> Ed25519 (v2, current)
    //    65-byte key  -> brainpoolp256r1 SHA256withECDSA (v1, deprecated legacy light clients)
    if (!verifySignature(ohAuthPublicKey, signingBytes, signature)) {
      return AuthResult.INVALID_SIGNATURE;
    }

    // Auth successful -> store nonce to prevent replay
    replayCache.put(replayKey, timestampMs);

    // Cleanup cache occasionally (simple strategy: remove entries older than
    // window)
    // NOTE: This is a lazy cleanup for PoC.
    if (replayCache.size() > 10000) {
      cleanupCache(now);
    }

    return AuthResult.OK;
  }

  private void cleanupCache(long now) {
    replayCache.entrySet().removeIf(entry -> Math.abs(now - entry.getValue()) > TIME_WINDOW_MS);
  }

  /**
   * Verifies the signature with the algorithm selected by the public key length. Ed25519 (32-byte
   * key, 64-byte signature) is the MS03 format; the brainpool ECDSA path only exists for legacy
   * (pre-MS03) light clients and is removed together with protocol-v22 support.
   */
  @SuppressWarnings("deprecation")
  private boolean verifySignature(byte[] ohAuthPublicKey, byte[] signingBytes, byte[] signature) {
    try {
      if (ohAuthPublicKey.length == NodeId.KEY_COMPONENT_LEN) {
        // Ed25519: clients send only the 32-byte verify key as oh_auth_public_key
        org.bouncycastle.crypto.params.Ed25519PublicKeyParameters verifyKey =
            new org.bouncycastle.crypto.params.Ed25519PublicKeyParameters(ohAuthPublicKey, 0);
        if (signature == null || signature.length != NodeId.SIGNATURE_LEN) {
          logger.warn("OutboundAuth: invalid Ed25519 signature length");
          return false;
        }
        org.bouncycastle.crypto.signers.Ed25519Signer signer =
            new org.bouncycastle.crypto.signers.Ed25519Signer();
        signer.init(false, verifyKey);
        signer.update(signingBytes, 0, signingBytes.length);
        if (!signer.verifySignature(signature)) {
          logger.warn("OutboundAuth: Ed25519 signature verification failed");
          return false;
        }
        return true;
      }

      if (ohAuthPublicKey.length == im.redpanda.crypt.legacy.LegacyNodeId.PUBLIC_KEYLEN) {
        im.redpanda.crypt.legacy.LegacyNodeId verifier =
            im.redpanda.crypt.legacy.LegacyNodeId.importPublic(ohAuthPublicKey);
        if (!verifier.verify(signingBytes, signature)) {
          logger.warn("OutboundAuth: legacy ECDSA signature verification failed");
          return false;
        }
        return true;
      }

      logger.warn("OutboundAuth: unsupported public key length {} bytes", ohAuthPublicKey.length);
      return false;
    } catch (Exception e) {
      logger.warn("OutboundAuth: Signature verification error", e);
      return false;
    }
  }
}
