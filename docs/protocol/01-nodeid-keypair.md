Title: NodeId.setKeyPair Protocol and Guarantees

Summary
- Purpose: Ensure NodeId keypair setting is safe and consistent with existing identity.

Behavior
- Reject null: Calling setKeyPair(null) throws a RuntimeException.
- Reject duplicate set: Calling setKeyPair when a keypair is already present throws a RuntimeException.
- Require known KademliaId: If KademliaId is not known, setKeyPair throws a RuntimeException.
- Validate match: setKeyPair accepts only a keypair whose public key maps to the existing KademliaId; otherwise throws KeypairDoesNotMatchException.

Verification
- Unit test sets NodeId from a KademliaId and accepts the matching keypair.
- Unit test asserts null, duplicate, and mismatch cases throw as documented.

