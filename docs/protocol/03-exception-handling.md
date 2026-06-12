Title: Exception Handling Adjustments in Crypto

Summary
- Purpose: Align caught exceptions with actual behavior after the MS03 crypto migration.

Behavior
- Removed ShortBufferException from catch lists where buffer-based update/doFinal is not used.
- CryptoUtils encryptGcm/decryptGcm propagate GeneralSecurityException; a failed GCM
  authentication surfaces as AEADBadTagException.
- GarlicMessage v2 catches AEADBadTagException during parseContent and drops the packet.
- GcmFramedStreams maps authentication/framing failures to PeerProtocolException, which
  disconnects the peer.

Verification
- Unit tests verify that tampered garlic payloads and TCP frames fail with the documented
  exceptions and that no plaintext is produced.
