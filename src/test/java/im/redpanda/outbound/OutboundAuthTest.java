package im.redpanda.outbound;

import static org.junit.Assert.assertEquals;

import im.redpanda.core.NodeId;
import java.security.KeyPair;
import org.junit.Before;
import org.junit.Test;

public class OutboundAuthTest {

  private OutboundAuth auth;
  private NodeId clientNode;

  @Before
  public void setUp() {
    auth = new OutboundAuth();
    // Generate a valid keypair for testing
    KeyPair keyPair = NodeId.generateECKeys();
    clientNode = new NodeId(keyPair);
  }

  @Test
  public void testVerify_Valid() {
    long timestamp = System.currentTimeMillis();
    byte[] ohId = clientNode.getKademliaId().getBytes(); // In real usage, oh_id is this
    // For test, oh_id doesn't matter for signature verification logic unless we
    // enforce it matches public key hash?
    // OutboundAuth logic: verify(ohAuthPublicKey, signingBytes, signature,
    // timestamp, ohId, nonce)
    // signingBytes in protocol: (command(1) + oh_id + expires + timestamp + nonce +
    // [payload])
    // But OutboundAuth.verify just checks signature of 'signingBytes' against
    // 'ohAuthPublicKey'.
    // It implies 'signingBytes' are what was signed.

    byte[] payload = "testPayload".getBytes();
    byte[] nonce = "nonce1".getBytes();

    // Create signature
    byte[] signature = clientNode.sign(payload);

    OutboundAuth.AuthResult result =
        auth.verify(clientNode.exportPublic(), payload, signature, timestamp, ohId, nonce);

    assertEquals(OutboundAuth.AuthResult.OK, result);
  }

  @Test
  public void testVerify_InvalidSignature() {
    long timestamp = System.currentTimeMillis();
    byte[] payload = "testPayload".getBytes();
    byte[] nonce = "nonce2".getBytes();
    byte[] ohId = clientNode.getKademliaId().getBytes();

    // Sign payload
    byte[] signature = clientNode.sign(payload);

    // Verify with DIFFERENT payload
    byte[] tamperedPayload = "tampered".getBytes();

    OutboundAuth.AuthResult result =
        auth.verify(clientNode.exportPublic(), tamperedPayload, signature, timestamp, ohId, nonce);

    assertEquals(OutboundAuth.AuthResult.INVALID_SIGNATURE, result);
  }

  @Test
  public void testVerify_InvalidTimestamp() {
    long now = System.currentTimeMillis();
    long oldTimestamp = now - (10 * 60 * 1000); // 10 mins ago (window is 5 mins)

    byte[] payload = "testPayload".getBytes();
    byte[] nonce = "nonce3".getBytes();
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] signature =
        clientNode.sign(payload); // Sig is valid, but timestamp logic is separate check

    OutboundAuth.AuthResult result =
        auth.verify(clientNode.exportPublic(), payload, signature, oldTimestamp, ohId, nonce);

    assertEquals(OutboundAuth.AuthResult.INVALID_TIMESTAMP, result);
  }

  @Test
  public void testVerify_Replay() {
    long timestamp = System.currentTimeMillis();
    byte[] payload = "testPayload".getBytes();
    byte[] nonce = "nonceReplay".getBytes();
    byte[] ohId = clientNode.getKademliaId().getBytes();
    byte[] signature = clientNode.sign(payload);

    // First call -> OK
    OutboundAuth.AuthResult result1 =
        auth.verify(clientNode.exportPublic(), payload, signature, timestamp, ohId, nonce);
    assertEquals(OutboundAuth.AuthResult.OK, result1);

    // Second call same params -> REPLAY
    OutboundAuth.AuthResult result2 =
        auth.verify(clientNode.exportPublic(), payload, signature, timestamp, ohId, nonce);
    assertEquals(OutboundAuth.AuthResult.REPLAY, result2);
  }
}
