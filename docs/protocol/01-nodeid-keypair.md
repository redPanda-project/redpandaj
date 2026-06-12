Title: NodeId.setKeys Protocol and Guarantees

Summary
- Purpose: Ensure NodeId key setting is safe and consistent with existing identity.
- Since MS03 a NodeId consists of an Ed25519 signing keypair and an X25519 encryption keypair
  (key separation); the KademliaId is SHA-256 of the 32-byte Ed25519 verify key.

Behavior
- Reject null: Calling setKeys(null) or with a key-less NodeId throws a RuntimeException.
- Reject duplicate set: Calling setKeys when keys are already present throws a RuntimeException.
- Require known KademliaId: If KademliaId is not known, setKeys throws a RuntimeException.
- Validate match: setKeys accepts only keys whose verify key maps to the existing KademliaId; otherwise throws KeypairDoesNotMatchException.

Verification
- Unit test sets NodeId from a KademliaId and accepts the matching keys.
- Unit test asserts null, duplicate, and mismatch cases throw as documented.
