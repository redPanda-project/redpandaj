Title: Exception Handling Adjustments in Crypto

Summary
- Purpose: Align caught exceptions with actual behavior after crypto refactor.

Behavior
- Removed ShortBufferException from catch lists where buffer-based update/doFinal is not used.
- Methods encryptString and decryptString catch algorithm/parameter/encoding exceptions relevant to AES/GCM use.

Verification
- Unit test compiles against current signatures and verifies:
  - Too-short inputs return null (length guard).
  - Malformed but length-valid inputs are handled internally and return null (AEAD tag failure is caught in decryptString).
