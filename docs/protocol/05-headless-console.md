Title: Headless Console Behavior in Tests

Summary
- Purpose: Allow benign NPE from ListenConsole when stdin is absent in headless runners.

Behavior
- The test harness tolerates a specific NullPointerException in logs originating from ListenConsole in headless mode to prevent false failures.

Verification
- E2E logs allowlist includes the headless console NPE; test asserts logs otherwise free of unexpected ERROR/WARN/Exception lines.

