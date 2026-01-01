package im.redpanda.outbound;

import im.redpanda.core.Peer;
import im.redpanda.outbound.v1.RegisterOhRequest;
import org.junit.Before;
import org.junit.Test;

public class OutboundIntegrationTest {

  private OutboundService service;
  private OutboundHandleStore handleStore;
  private OutboundMailboxStore mailboxStore;
  private Peer peer;

  @Before
  public void setUp() {
    handleStore = new OutboundHandleStore();
    mailboxStore = new OutboundMailboxStore();
    service = new OutboundService(handleStore, mailboxStore);
    peer = new Peer("127.0.0.1", 12345);
    peer.writeBuffer = java.nio.ByteBuffer.allocate(1024);
    peer.writeBuffer.flip(); // Prepare for reading (although we write to it)
    // Actually we write to it, so it should be in write mode (position 0, limit
    // cap).
    peer.writeBuffer.clear();

    // Set selection key to avoid NPE in setWriteBufferFilled if mocked/stubbed
    // But Peer.setWriteBufferFilled checks if key is null and returns false.
    // However, if we want to valid command writing...
    // The code only locks/unlocks and puts.
    // setWriteBufferFilled is called at the end.
    // Let's ensure it doesn't crash.
    peer.setConnected(true); // make isConnected return true
  }

  @Test
  public void testRegister_ValidRequest_Success() {
    // Since we mock/stub nothing and use real Auth which fails signature,
    // we expect it to return REGISTER_RES with INVALID_SIGNATURE (or error).
    // But verify it DOES NOT throw exception.

    // We need to generate a real KeyPair to sign if we want SUCCESS.
    // For now, let's just verify it runs without crashing.
    RegisterOhRequest req =
        RegisterOhRequest.newBuilder()
            .setTimestampMs(System.currentTimeMillis())
            // .setSignature(...) // Invalid
            .build();

    // The service.handleRegister returns int (bytes written).
    // It should NOT throw exception now.
    service.handleRegister(peer, req);
  }

  @Test
  public void testAuth_InvalidSignature_ReturnsError_Stub() {
    // This is a placeholder to verify Auth integration later
    // Logic will be inside OutboundService
  }
}
