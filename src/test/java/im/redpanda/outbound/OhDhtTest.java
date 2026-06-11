package im.redpanda.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.kademlia.KadContent;
import im.redpanda.outbound.v1.OhNodeRecord;
import java.security.SecureRandom;
import java.util.List;
import org.junit.Test;

/**
 * MS02b: tests for the OH → host-node DHT discovery primitives — deterministic announce-key
 * derivation, fixed-size (padded) records, and validation of resolve results.
 */
public class OhDhtTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  private static byte[] randomOhId() {
    byte[] ohId = new byte[20];
    RANDOM.nextBytes(ohId);
    return ohId;
  }

  private static KademliaId randomNodeKadId() {
    byte[] bytes = new byte[KademliaId.ID_LENGTH_BYTES];
    RANDOM.nextBytes(bytes);
    return new KademliaId(bytes);
  }

  // --- Announce key derivation ---

  @Test
  public void deriveAnnounceNodeId_isDeterministic() {
    byte[] ohId = randomOhId();

    NodeId first = OhDht.deriveAnnounceNodeId(ohId);
    NodeId second = OhDht.deriveAnnounceNodeId(ohId);

    assertThat(first.exportPublic()).isEqualTo(second.exportPublic());
    assertThat(first.getKademliaId()).isEqualTo(second.getKademliaId());
  }

  @Test
  public void deriveAnnounceNodeId_differsPerOhId() {
    NodeId first = OhDht.deriveAnnounceNodeId(randomOhId());
    NodeId second = OhDht.deriveAnnounceNodeId(randomOhId());

    assertThat(first.exportPublic()).isNotEqualTo(second.exportPublic());
  }

  @Test
  public void deriveAnnounceNodeId_exportsWireCompatiblePublicKeyAndSigns() {
    NodeId announceId = OhDht.deriveAnnounceNodeId(randomOhId());

    // Same public key length as regular NodeIds → fits all existing wire formats
    byte[] publicKey = announceId.exportPublic();
    assertThat(publicKey).hasSize(NodeId.PUBLIC_KEYLEN);

    // Sign with the derived private key, verify against the re-imported public key
    byte[] message = "announce-test".getBytes();
    byte[] signature = announceId.sign(message);
    NodeId imported = NodeId.importPublic(publicKey);
    assertThat(imported.verify(message, signature)).isTrue();
  }

  @Test
  public void announceKademliaId_sameForEveryoneKnowingOhId() {
    byte[] ohId = randomOhId();
    long timestamp = System.currentTimeMillis();

    assertThat(OhDht.announceKademliaId(ohId, timestamp))
        .isEqualTo(OhDht.announceKademliaId(ohId.clone(), timestamp));
  }

  // --- Announce record building (padding) ---

  @Test
  public void buildAnnounceContent_recordsHaveConstantSize() {
    long now = System.currentTimeMillis();
    int expectedSize = OhDht.RECORD_SIZE_BYTES;

    for (int i = 0; i < 20; i++) {
      KadContent content = OhDht.buildAnnounceContent(randomOhId(), randomNodeKadId(), now);
      assertThat(content.getContent())
          .as("every announce record must be padded to the same size")
          .hasSize(expectedSize);
    }
  }

  @Test
  public void buildAnnounceContent_isSignedAndStoredUnderDerivedKey() {
    byte[] ohId = randomOhId();
    KademliaId hostNode = randomNodeKadId();
    long now = System.currentTimeMillis();

    KadContent content = OhDht.buildAnnounceContent(ohId, hostNode, now);

    assertThat(content.verify()).isTrue();
    assertThat(content.getId()).isEqualTo(OhDht.announceKademliaId(ohId, now));
    assertThat(content.getPubkey()).isEqualTo(OhDht.deriveAnnounceNodeId(ohId).exportPublic());
  }

  // --- Resolve-result validation ---

  @Test
  public void extractValidRecord_returnsRecordWithHostNode() throws Exception {
    byte[] ohId = randomOhId();
    KademliaId hostNode = randomNodeKadId();
    long now = System.currentTimeMillis();

    KadContent content = OhDht.buildAnnounceContent(ohId, hostNode, now);
    OhNodeRecord record = OhDht.extractValidRecord(List.of(content), ohId, now);

    assertThat(record).isNotNull();
    assertThat(record.getNodeId().toByteArray()).isEqualTo(hostNode.getBytes());
    assertThat(record.getOhIdHash().toByteArray()).isEqualTo(Sha256Hash.create(ohId).getBytes());
  }

  @Test
  public void extractValidRecord_picksNewestValidRecord() {
    byte[] ohId = randomOhId();
    long now = System.currentTimeMillis();
    KademliaId oldNode = randomNodeKadId();
    KademliaId newNode = randomNodeKadId();

    KadContent older = OhDht.buildAnnounceContent(ohId, oldNode, now - 10_000);
    KadContent newer = OhDht.buildAnnounceContent(ohId, newNode, now);

    OhNodeRecord record = OhDht.extractValidRecord(List.of(older, newer), ohId, now);

    assertThat(record).isNotNull();
    assertThat(record.getNodeId().toByteArray())
        .as("the record with the newest timestamp wins (OH migration)")
        .isEqualTo(newNode.getBytes());
  }

  @Test
  public void extractValidRecord_rejectsRecordSignedByForeignKey() {
    byte[] ohId = randomOhId();
    byte[] otherOhId = randomOhId();
    long now = System.currentTimeMillis();

    // Record announced for ANOTHER oh_id (signed by a different derived key)
    KadContent foreign = OhDht.buildAnnounceContent(otherOhId, randomNodeKadId(), now);

    assertThat(OhDht.extractValidRecord(List.of(foreign), ohId, now)).isNull();
  }

  @Test
  public void extractValidRecord_rejectsTamperedOhIdHash() throws Exception {
    byte[] ohId = randomOhId();
    long now = System.currentTimeMillis();

    // Forge a record claiming the right oh_id_hash is something else; even when signed with the
    // correct derived key, a mismatching hash must be rejected.
    NodeId announceNodeId = OhDht.deriveAnnounceNodeId(ohId);
    OhNodeRecord forgedRecord =
        OhNodeRecord.newBuilder()
            .setOhIdHash(ByteString.copyFrom(new byte[32]))
            .setNodeId(ByteString.copyFrom(randomNodeKadId().getBytes()))
            .setAnnouncedAtMs(now)
            .build();
    KadContent forged =
        new KadContent(now, announceNodeId.exportPublic(), forgedRecord.toByteArray());
    forged.signWith(announceNodeId);

    assertThat(OhDht.extractValidRecord(List.of(forged), ohId, now)).isNull();
  }

  @Test
  public void extractValidRecord_rejectsNonPaddedRecord() {
    byte[] ohId = randomOhId();
    long now = System.currentTimeMillis();

    // Correctly derived key but content without the fixed-size padding — must be rejected
    // (uniform record size is part of the anti-profiling contract)
    NodeId announceNodeId = OhDht.deriveAnnounceNodeId(ohId);
    OhNodeRecord unpadded =
        OhNodeRecord.newBuilder()
            .setOhIdHash(ByteString.copyFrom(Sha256Hash.create(ohId).getBytes()))
            .setNodeId(ByteString.copyFrom(randomNodeKadId().getBytes()))
            .setAnnouncedAtMs(now)
            .build();
    KadContent content = new KadContent(now, announceNodeId.exportPublic(), unpadded.toByteArray());
    content.signWith(announceNodeId);

    assertThat(OhDht.extractValidRecord(List.of(content), ohId, now)).isNull();
  }

  @Test
  public void extractValidRecord_rejectsStaleRecord() {
    byte[] ohId = randomOhId();
    long now = System.currentTimeMillis();

    KadContent stale =
        OhDht.buildAnnounceContent(ohId, randomNodeKadId(), now - OhDht.MAX_RECORD_AGE_MS - 1);

    assertThat(OhDht.extractValidRecord(List.of(stale), ohId, now)).isNull();
  }

  @Test
  public void extractValidRecord_emptyOrNullInput_returnsNull() {
    byte[] ohId = randomOhId();
    long now = System.currentTimeMillis();

    assertThat(OhDht.extractValidRecord(List.of(), ohId, now)).isNull();
    assertThat(OhDht.extractValidRecord(null, ohId, now)).isNull();
  }
}
