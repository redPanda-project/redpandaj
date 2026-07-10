package im.redpanda.outbound;

import im.redpanda.core.NodeId;
import im.redpanda.crypt.Utils;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

  /**
   * Validity window for signed outbound commands (timestamps up to this far in the past OR future
   * are accepted — clock-skew tolerance); also the replay-cache retention time. The cache is not
   * persisted across restarts (accepted residual risk: replay of a command captured within the
   * window during the seconds of a node restart — see sdd02 decision 3). Package-private for tests.
   */
  static final long TIME_WINDOW_MS = 5L * 60L * 1000L;

  /** How often at most {@link #cleanupCache(long)} runs (time-based, not size-triggered). */
  private static final long CLEANUP_INTERVAL_MS = 60_000L;

  /**
   * Maximum replay-cache entries per source (oh_id) within the retention window. When reached,
   * further commands from that source are rejected — never evicted, so flooding one source can
   * neither displace other sources' entries nor reopen the replay window for its own.
   * Package-private for tests.
   */
  static final int MAX_ENTRIES_PER_SOURCE = 1_000;

  /** Safety valve only (forces an early cleanup); not reached in normal operation. */
  private static final int GLOBAL_SAFETY_LIMIT = 100_000;

  // Replay cache: (ohIdHex + "_" + nonceHex) -> command timestamp
  private final ConcurrentHashMap<String, Long> replayCache = new ConcurrentHashMap<>();

  /**
   * Entries per source (key = ohIdHex); rebuilt from scratch on every cleanup. Counters may be off
   * by a few under concurrent verifies — acceptable, the cap is a flood limit, not an exact quota.
   */
  private final ConcurrentHashMap<String, AtomicInteger> entriesPerSource =
      new ConcurrentHashMap<>();

  private final AtomicLong lastCleanupMs = new AtomicLong(0);

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

    // 2. Check Replay — fast path only; the authoritative, race-free check is the putIfAbsent
    // below (two threads passing containsKey concurrently must not both get accepted)
    String ohIdHex = Utils.bytesToHexString(ohId);
    String replayKey = ohIdHex + "_" + Utils.bytesToHexString(nonce);
    if (replayCache.containsKey(replayKey)) {
      logger.warn("OutboundAuth: Replay detected for key {}", replayKey);
      return AuthResult.REPLAY;
    }

    // 3. Verify Signature — dual-version support (MS03 transition):
    //    32-byte key  -> Ed25519 over [0x02 | signingBytes]      (v2, current)
    //    65-byte key  -> brainpool SHA256withECDSA, no version    (v1, deprecated light clients)
    if (!verifySignature(ohAuthPublicKey, signingBytes, signature)) {
      return AuthResult.INVALID_SIGNATURE;
    }

    // Time-based cleanup, at most once per interval; the CAS keeps concurrent verifies from
    // cleaning up in parallel. Runs before the cap check so a capped source recovers as soon as
    // its entries have expired.
    long last = lastCleanupMs.get();
    if ((now - last > CLEANUP_INTERVAL_MS || replayCache.size() > GLOBAL_SAFETY_LIMIT)
        && lastCleanupMs.compareAndSet(last, now)) {
      cleanupCache(now);
    }

    // Per-source cap: reject, never evict (evicting would reopen the replay window — an attacker
    // could flood until an earlier command of his is displaced and then replay it). With no free
    // slot the nonce cannot be recorded, so freshness cannot be guaranteed -> REPLAY.
    AtomicInteger sourceCount = entriesPerSource.computeIfAbsent(ohIdHex, k -> new AtomicInteger());
    if (sourceCount.get() >= MAX_ENTRIES_PER_SOURCE) {
      logger.warn("OutboundAuth: per-source replay-cache cap reached for {}, rejecting", ohIdHex);
      return AuthResult.REPLAY;
    }
    sourceCount.incrementAndGet();

    // Auth successful -> record the nonce atomically; putIfAbsent guarantees at most one caller
    // ever gets OK for a given (ohId, nonce), even if both raced past the containsKey above
    if (replayCache.putIfAbsent(replayKey, timestampMs) != null) {
      sourceCount.decrementAndGet();
      logger.warn("OutboundAuth: Replay detected for key {}", replayKey);
      return AuthResult.REPLAY;
    }

    return AuthResult.OK;
  }

  /**
   * Removes entries whose timestamp lies outside the ±{@link #TIME_WINDOW_MS} window around {@code
   * now} — past or future, matching the clock-skew semantics of the timestamp check — and rebuilds
   * the per-source counters from the remaining entries. Triggered from {@link #verify} at most once
   * per {@link #CLEANUP_INTERVAL_MS} (or early via {@link #GLOBAL_SAFETY_LIMIT}). The rebuild is
   * O(n) once per interval — deliberately simpler than incremental bookkeeping, and empty sources
   * disappear automatically. Package-private for tests.
   */
  void cleanupCache(long now) {
    replayCache.entrySet().removeIf(entry -> Math.abs(now - entry.getValue()) > TIME_WINDOW_MS);
    entriesPerSource.clear();
    for (String key : replayCache.keySet()) {
      String source = key.substring(0, key.indexOf('_'));
      entriesPerSource.computeIfAbsent(source, k -> new AtomicInteger()).incrementAndGet();
    }
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
