Title: Testing Strategy Enhancements

Summary
- Purpose: Add targeted unit tests and Failsafe-based E2E integration tests.

Behavior
- Unit tests: Focused tests for NodeId.setKeyPair and ECCrypto.
- E2E: TwoNodesE2EIT runs as an IT via Maven Failsafe in a dedicated e2e profile.

Verification
- Unit tests expose and prevent regressions; IT verifies nodes start, log health, and stop.

