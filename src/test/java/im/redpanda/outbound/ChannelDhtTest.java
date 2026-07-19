package im.redpanda.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import im.redpanda.kademlia.KadContent;
import java.security.SecureRandom;
import java.util.List;
import org.junit.Test;

/**
 * T43: tests for the channel-rendezvous DHT primitives — deterministic record-key derivation from
 * the channel secret, fixed-size padded (opaque) records, TTL / signature / size validation and
 * newest-wins result selection.
 */
public class ChannelDhtTest {

  private static final SecureRandom RANDOM = new SecureRandom();

  private static byte[] randomChannelSecret() {
    byte[] secret = new byte[32];
    RANDOM.nextBytes(secret);
    return secret;
  }

  private static byte[] randomCiphertext(int len) {
    byte[] ciphertext = new byte[len];
    RANDOM.nextBytes(ciphertext);
    return ciphertext;
  }

  // --- Record key derivation ---

  @Test
  public void deriveRecordNodeId_isDeterministic() {
    byte[] secret = randomChannelSecret();

    NodeId first = ChannelDht.deriveRecordNodeId(secret);
    NodeId second = ChannelDht.deriveRecordNodeId(secret.clone());

    assertThat(first.exportPublic()).isEqualTo(second.exportPublic());
    assertThat(first.getKademliaId()).isEqualTo(second.getKademliaId());
  }

  @Test
  public void deriveRecordNodeId_differsPerChannel() {
    NodeId first = ChannelDht.deriveRecordNodeId(randomChannelSecret());
    NodeId second = ChannelDht.deriveRecordNodeId(randomChannelSecret());

    assertThat(first.exportPublic()).isNotEqualTo(second.exportPublic());
  }

  @Test
  public void rendezvousKademliaId_sameForEveryParticipant() {
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();

    assertThat(ChannelDht.rendezvousKademliaId(secret, now))
        .isEqualTo(ChannelDht.rendezvousKademliaId(secret.clone(), now));
  }

  @Test
  public void rendezvousKademliaId_isDomainSeparatedFromOhAnnounce() {
    // Feeding the same 32 bytes as an oh_id vs a channel secret must land in different namespaces:
    // the channel record key is derived through a distinct domain tag.
    byte[] shared = randomChannelSecret();
    long now = System.currentTimeMillis();

    assertThat(ChannelDht.rendezvousKademliaId(shared, now))
        .isNotEqualTo(OhDht.announceKademliaId(shared, now));
  }

  // --- Record building (padding, signature, self-certifying key) ---

  @Test
  public void buildRecordContent_recordsHaveConstantSize() {
    long now = System.currentTimeMillis();
    for (int i = 0; i < 20; i++) {
      KadContent content =
          ChannelDht.buildRecordContent(
              randomChannelSecret(), randomCiphertext(1 + RANDOM.nextInt(200)), now);
      assertThat(content.getContent())
          .as("every rendezvous record must be padded to one bucket size")
          .hasSize(ChannelDht.RECORD_SIZE_BYTES);
    }
  }

  @Test
  public void buildRecordContent_isSignedAndStoredUnderDerivedKey() {
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();

    KadContent content = ChannelDht.buildRecordContent(secret, randomCiphertext(100), now);

    assertThat(content.verify()).isTrue();
    assertThat(content.getId()).isEqualTo(ChannelDht.rendezvousKademliaId(secret, now));
    assertThat(content.getPubkey()).isEqualTo(ChannelDht.deriveRecordNodeId(secret).exportPublic());
  }

  @Test
  public void buildAndExtractCiphertext_roundTrips() {
    byte[] ciphertext = randomCiphertext(137);
    KadContent content =
        ChannelDht.buildRecordContent(
            randomChannelSecret(), ciphertext, System.currentTimeMillis());

    assertThat(ChannelDht.extractCiphertext(content.getContent())).isEqualTo(ciphertext);
  }

  // --- Validation ---

  @Test
  public void isValidRecord_acceptsFreshSignedFixedSizeRecord() {
    long now = System.currentTimeMillis();
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomCiphertext(64), now);

    assertThat(ChannelDht.isValidRecord(content, now)).isTrue();
  }

  @Test
  public void isValidRecord_rejectsWrongSize() {
    // Correctly derived key but content that is not padded to the fixed bucket size.
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();
    NodeId recordNodeId = ChannelDht.deriveRecordNodeId(secret);
    KadContent content = new KadContent(now, recordNodeId.exportPublic(), randomCiphertext(100));
    content.signWith(recordNodeId);

    assertThat(ChannelDht.isValidRecord(content, now)).isFalse();
  }

  @Test
  public void isValidRecord_rejectsTamperedContent() {
    long now = System.currentTimeMillis();
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomCiphertext(64), now);
    // Flip a byte after signing → signature no longer verifies.
    content.getContent()[0] ^= 0xFF;

    assertThat(ChannelDht.isValidRecord(content, now)).isFalse();
  }

  @Test
  public void isValidRecord_rejectsRecordOlderThanTtl() {
    long now = System.currentTimeMillis();
    long published = now - ChannelDht.MAX_RECORD_AGE_MS - 1000;
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomCiphertext(64), published);

    assertThat(ChannelDht.isValidRecord(content, now)).isFalse();
  }

  // --- Result selection (newest-wins, foreign-record rejection) ---

  @Test
  public void extractNewest_picksNewestValidRecord() {
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();
    KademliaId key = ChannelDht.rendezvousKademliaId(secret, now);

    KadContent older = ChannelDht.buildRecordContent(secret, randomCiphertext(64), now - 10_000);
    KadContent newer = ChannelDht.buildRecordContent(secret, randomCiphertext(64), now);
    // Both live under today's key.
    assertThat(older.getId()).isEqualTo(key);
    assertThat(newer.getId()).isEqualTo(key);

    KadContent best = ChannelDht.extractNewest(List.of(older, newer), key, now);

    assertThat(best).isNotNull();
    assertThat(best.getTimestamp()).isEqualTo(newer.getTimestamp());
  }

  @Test
  public void extractNewest_rejectsRecordUnderDifferentKey() {
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();
    KademliaId key = ChannelDht.rendezvousKademliaId(secret, now);

    // A validly signed record for ANOTHER channel must not be served for this key.
    KadContent foreign =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomCiphertext(64), now);

    assertThat(ChannelDht.extractNewest(List.of(foreign), key, now)).isNull();
  }

  @Test
  public void extractNewest_returnsNullForEmptyResults() {
    long now = System.currentTimeMillis();
    KademliaId key = ChannelDht.rendezvousKademliaId(randomChannelSecret(), now);

    assertThat(ChannelDht.extractNewest(List.of(), key, now)).isNull();
  }
}
