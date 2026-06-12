package im.redpanda.outbound;

import im.redpanda.core.NodeId;
import im.redpanda.crypt.Utils;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OutboundAuth {

  private static final Logger logger = LogManager.getLogger();

  /**
   * Signing-byte format version (MS03, master spec §8): every Ed25519 signature covers {@code
   * [SIGNING_VERSION_ED25519 | CMD_BYTE | command-specific fields | timestamp | nonce]}. The
   * pre-MS03 v1 format (legacy brainpool ECDSA) carries no version byte: {@code [CMD_BYTE | ...]}.
   * Future algorithm changes bump this byte, so commands can be verified dual-version during a
   * transition instead of a big-bang cutover.
   */
  public static final byte SIGNING_VERSION_ED25519 = 0x02;

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

  /**
   * Verifies a signed outbound command. {@code signingBytes} is the unversioned command body {@code
   * [CMD_BYTE | command-specific fields | timestamp | nonce]}; the version/algorithm prefix
   * required by the v2 (Ed25519) signing-byte format is applied here, in one place for all signed
   * commands (register/fetch/revoke/ackFetch and future ones).
   */
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
    //    32-byte key  -> Ed25519 over [0x02 | signingBytes]      (v2, current)
    //    65-byte key  -> brainpool SHA256withECDSA, no version    (v1, deprecated light clients)
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
   * Verifies the signature with the algorithm and signing-byte version selected by the public key
   * length. Ed25519 (32-byte key, 64-byte signature) is the MS03 format and signs the versioned
   * bytes {@code [0x02 | signingBytes]}; the brainpool ECDSA path signs the unversioned v1 bytes
   * and only exists for legacy (pre-MS03) light clients — removed together with protocol-v22
   * support.
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
        // v2 signing-byte format: 1-byte version/algorithm prefix before CMD_BYTE
        signer.update(SIGNING_VERSION_ED25519);
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
