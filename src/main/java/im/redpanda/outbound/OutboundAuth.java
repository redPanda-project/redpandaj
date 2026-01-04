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

    // 3. Verify Signature
    NodeId verifier;
    try {
      verifier = NodeId.importPublic(ohAuthPublicKey);
    } catch (Exception e) {
      logger.warn("OutboundAuth: Could not import public key", e);
      return AuthResult.INVALID_SIGNATURE;
    }

    if (verifier == null) {
      logger.warn("OutboundAuth: Could not import public key");
      return AuthResult.INVALID_SIGNATURE; // Malformed public key
    }

    try {
      if (!verifier.verify(signingBytes, signature)) {
        logger.warn("OutboundAuth: Signature verification failed");
        return AuthResult.INVALID_SIGNATURE;
      }
    } catch (Exception e) {
      logger.warn("OutboundAuth: Signature verification error", e);
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
}
