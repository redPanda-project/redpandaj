package im.redpanda.core;

import static com.google.protobuf.ByteString.copyFrom;
import static org.junit.Assert.*;

import com.google.protobuf.ByteString;
import im.redpanda.outbound.OutboundHandleStore;
import im.redpanda.outbound.OutboundHandleStore.HandleRecord;
import im.redpanda.outbound.OutboundMailboxStore;
import im.redpanda.outbound.OutboundService;
import im.redpanda.outbound.v1.MailItem;
import im.redpanda.proto.FlaschenpostPut;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@code oh_id} routing path introduced in MS01 for {@code FLASCHENPOST_PUT} handling
 * in {@link InboundCommandProcessor}.
 *
 * <p>Covers: direct-OH deposit when {@code oh_id} is present and registered, fall-through to legacy
 * GMParser when deposit is not applicable, and backward compatibility when {@code oh_id} is absent.
 */
public class InboundCommandProcessorFlaschenpostPutTest {

  private static final int HANDLE_KEY_LENGTH = 32;
  private static final long HANDLE_EXPIRY_MILLIS = 60_000;

  private ServerContext ctx;
  private InboundCommandProcessor proc;
  private OutboundHandleStore handleStore;
  private OutboundMailboxStore mailboxStore;

  @Before
  public void setup() {
    ctx = ServerContext.buildDefaultServerContext();
    handleStore = new OutboundHandleStore();
    mailboxStore = new OutboundMailboxStore();
    OutboundService outboundService = new OutboundService(handleStore, mailboxStore);
    ctx.setOutboundService(outboundService);
    proc = new InboundCommandProcessor(ctx);
  }

  /** Builds a [4-byte len][payload] frame as expected by {@code parseCommand}. */
  private static ByteBuffer buildFrame(byte[] payload) {
    ByteBuffer buf = ByteBuffer.allocate(4 + payload.length);
    buf.putInt(payload.length);
    buf.put(payload);
    buf.flip();
    return buf;
  }

  /** Returns a 20-byte oh_id suitable for registration and deposit tests. */
  private static byte[] sampleOhId() {
    byte[] id = new byte[KademliaId.ID_LENGTH_BYTES];
    for (int i = 0; i < id.length; i++) {
      id[i] = (byte) (i + 1);
    }
    return id;
  }

  /** Registers an OH handle with a future expiry in the in-memory handle store. */
  private void registerOh(byte[] ohId) {
    long now = System.currentTimeMillis();
    HandleRecord record =
        new HandleRecord(new byte[HANDLE_KEY_LENGTH], now, now + HANDLE_EXPIRY_MILLIS);
    handleStore.put(ohId, record);
  }

  /**
   * When {@code oh_id} is present and matches a registered OH, the message is deposited directly
   * into the mailbox and the handler returns early (GMParser is not invoked).
   */
  @Test
  public void flaschenpostPut_withValidRegisteredOhId_depositsToMailbox() {
    byte[] ohId = sampleOhId();
    registerOh(ohId);

    byte[] content = "direct-oh-payload".getBytes(StandardCharsets.UTF_8);
    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(content))
            .setOhId(ByteString.copyFrom(ohId))
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9001, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);

    // Verify the message was deposited into the mailbox (confirms early return via oh_id path)
    List<MailItem> items = mailboxStore.fetchMessages(ohId, 10, 0);
    assertEquals(1, items.size());
    assertArrayEquals(content, items.get(0).getPayload().toByteArray());
  }

  /**
   * When {@code oh_id} is present but no matching OH is registered (deposit returns false), the
   * handler falls through to the legacy GMParser path and completes without error.
   */
  @Test
  public void flaschenpostPut_withOhIdButNoRegisteredOh_fallsThroughToLegacy() {
    byte[] ohId = sampleOhId();
    // Intentionally skip registerOh() so depositMessage returns false

    // Use a valid GMAck payload so the legacy GMParser path handles it gracefully
    ByteBuffer ack = ByteBuffer.allocate(1 + 4 + 4);
    ack.put(im.redpanda.flaschenpost.GMType.ACK.getId());
    ack.putInt(4);
    ack.putInt(99);
    ack.flip();
    byte[] ackBytes = new byte[ack.remaining()];
    ack.get(ackBytes);

    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(ackBytes))
            .setOhId(ByteString.copyFrom(ohId))
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9002, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);

    // Mailbox must be empty – deposit was not performed
    List<MailItem> items = mailboxStore.fetchMessages(ohId, 10, 0);
    assertEquals(0, items.size());
  }

  /**
   * When {@code oh_id} has an invalid byte length, the handler logs a warning and falls through to
   * the legacy GMParser path without throwing.
   */
  @Test
  public void flaschenpostPut_withInvalidOhIdLength_fallsThroughToLegacy() {
    byte[] wrongLengthId = new byte[5]; // too short, not ID_LENGTH_BYTES

    ByteBuffer ack = ByteBuffer.allocate(1 + 4 + 4);
    ack.put(im.redpanda.flaschenpost.GMType.ACK.getId());
    ack.putInt(4);
    ack.putInt(7);
    ack.flip();
    byte[] ackBytes = new byte[ack.remaining()];
    ack.get(ackBytes);

    FlaschenpostPut putMsg =
        FlaschenpostPut.newBuilder()
            .setContent(copyFrom(ackBytes))
            .setOhId(ByteString.copyFrom(wrongLengthId))
            .build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9003, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);
  }

  /**
   * When {@code oh_id} is absent (pre-MS01 message), the handler uses the legacy GMParser path.
   * This ensures backward compatibility with messages that do not carry an explicit {@code oh_id}.
   */
  @Test
  public void flaschenpostPut_withoutOhId_usesLegacyPathWithoutError() {
    // Build GMAck payload with no oh_id field – mimics pre-MS01 behavior
    ByteBuffer ack = ByteBuffer.allocate(1 + 4 + 4);
    ack.put(im.redpanda.flaschenpost.GMType.ACK.getId());
    ack.putInt(4);
    ack.putInt(55);
    ack.flip();
    byte[] ackBytes = new byte[ack.remaining()];
    ack.get(ackBytes);

    FlaschenpostPut putMsg = FlaschenpostPut.newBuilder().setContent(copyFrom(ackBytes)).build();
    byte[] putData = putMsg.toByteArray();

    Peer peer = new Peer("127.0.0.1", 9004, ctx.getNodeId());
    peer.setConnected(true);
    ctx.getPeerList().add(peer);

    int consumed = proc.parseCommand(Command.FLASCHENPOST_PUT, buildFrame(putData), peer);

    assertEquals(1 + 4 + putData.length, consumed);
  }
}
