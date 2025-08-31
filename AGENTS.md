Purpose
- Summarize pitfalls and decisions made while enhancing tests, crypto, and e2e infra.

Key Pitfalls Found
- NodeId.setKeyPair logic: Inverted null check rejected valid key pairs; also lacked guard for already-set keypair. Fixed to (a) reject null input and already-initialized state, and (b) validate against existing KademliaId.
- ECCrypto IV reuse: Single static IV for AES/GCM caused identical ciphertexts for same plaintext and key. Fixed to use a fresh 16-byte IV per encryption and to encode IV||ciphertext for decryption.
- Catching unchecked exceptions: ECCrypto’s refactor removed buffer-based update/doFinal; ShortBufferException could no longer be thrown. Adjusted catch list to match actual behavior to restore compilation.
- E2E shutdown and storage: Forcible kills led to MapDB “Header checksum broken” warnings across runs. E2E now attempts graceful shutdown and isolates working dirs per run to avoid reusing data files.
- Headless console: ListenConsole throws NPE when stdin isn’t available in test runners. E2E allows this specific, benign NPE in logs to avoid false failures.

Testing Strategy
- Added focused unit tests to expose bugs (NodeIdSetKeyPairTest, ECCryptoTest) before applying fixes.
- Added TwoNodesE2EIT that runs two nodes, captures logs, asserts basic health, and stops them.
- Introduced Maven `e2e` profile using Failsafe to run `*IT.java` separately from unit tests.

Operational Notes
- Crypto format change: ECCrypto.encryptString now returns hex(IV||ciphertext). Decryption expects that format. Update external callers if any rely on the previous format.
- Log hygiene: E2E fails on unexpected ERROR/WARN/Exception lines by default, with narrow allowlist for first-run LocalSettings and headless console NPE.

Future Improvements
- Replace ListenConsole with non-blocking/optional console handling for service mode.
- Consider authenticated stream cipher (e.g., AES/GCM streaming via segments or ChaCha20-Poly1305) for Peer streams.
- Expand E2E: verify peering between nodes and simple message exchange once networking seeds are controlled.

