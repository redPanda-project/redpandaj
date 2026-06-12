Title: CryptoUtils AES-256-GCM Nonce Handling and Ciphertext Format

Summary
- Purpose: Prevent nonce reuse and define a stable authenticated ciphertext format (MS03).
- The pre-MS03 ECCrypto demo class (brainpool ECDH + hex string helpers) was removed.

Behavior
- Fresh nonce: every GarlicMessage v2 uses a new random 12-byte GCM nonce; TCP v23 frames use a
  per-direction 96-bit counter nonce that is never reused within a session.
- Encoding: AES-256-GCM ciphertexts always carry the 16-byte GCM tag appended (ciphertext || tag).
- AAD: garlic messages bind the ciphertext to the 20-byte destination KademliaId via the GCM
  additional authenticated data.
- Decryption: a wrong tag raises AEADBadTagException — never silently corrupted plaintext.

Verification
- Unit test: GarlicMessage v2 roundtrip; tampered ciphertext fails with AEADBadTagException.
- Unit test: GcmFramedStreams roundtrip; a flipped bit in a frame causes a decryption failure.
