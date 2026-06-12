Title: Operational Notes

Summary
- Crypto format change (MS03): authenticated payloads carry ciphertext || tag (AES-256-GCM); decryption verifies the tag before any plaintext is used.
- Log hygiene: E2E fails on unexpected ERROR/WARN/Exception lines with a narrow allowlist for first-run LocalSettings and headless console NPE.

Verification
- Unit tests ensure:
  - A correctly formatted payload decrypts.
  - Tampered payloads fail with AEADBadTagException / PeerProtocolException and yield no plaintext.
