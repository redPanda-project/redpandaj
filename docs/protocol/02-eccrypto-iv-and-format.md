Title: ECCrypto AES/GCM IV Handling and Ciphertext Format

Summary
- Purpose: Prevent IV reuse and define a stable ciphertext format.

Behavior
- Fresh IV: Each encryptString call generates a new 16-byte IV using SecureRandom.
- Encoding: encryptString returns hex(IV || ciphertext), where ciphertext includes the GCM tag.
- Decryption: decryptString expects hex(IV || ciphertext) and returns the original UTF-8 string.

Verification
- Unit test: Encrypting the same plaintext twice with the same key yields different ciphertexts.
- Unit test: decryptString(encryptString(key, text)) returns text.

