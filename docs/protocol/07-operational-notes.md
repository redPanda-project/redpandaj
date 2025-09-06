Title: Operational Notes

Summary
- Crypto format change: encryptString now returns hex(IV||ciphertext); decryption expects this format.
- Log hygiene: E2E fails on unexpected ERROR/WARN/Exception lines with a narrow allowlist for first-run LocalSettings and headless console NPE.

Verification
- Unit test ensures:
  - A correctly formatted payload decrypts.
  - Decrypting a payload without the IV prefix returns null (GCM auth failure is caught in code).
