Title: E2E Shutdown and Storage Isolation

Summary
- Purpose: Avoid MapDB reuse warnings and data leakage between E2E runs.

Behavior
- Graceful shutdown: E2E attempts to stop nodes cleanly before process termination.
- Isolated working dirs: Each E2E run uses a unique temporary directory to avoid reusing data files.

Verification
- E2E test starts two nodes, asserts basic health, and stops them without MapDB header checksum warnings on subsequent runs.

