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

  private static byte[] randomBytes(int len) {
    byte[] bytes = new byte[len];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  /** A fixed-size opaque record blob, as a client would produce (nonce + AEAD ciphertext). */
  private static byte[] randomRecordContent() {
    return randomBytes(ChannelDht.RECORD_SIZE_BYTES);
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
          ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), now);
      assertThat(content.getContent())
          .as("every rendezvous record is exactly one bucket size")
          .hasSize(ChannelDht.RECORD_SIZE_BYTES);
    }
  }

  @Test
  public void buildRecordContent_rejectsWrongSizeContent() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                ChannelDht.buildRecordContent(
                    randomChannelSecret(), randomBytes(100), System.currentTimeMillis()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void buildRecordContent_isSignedAndStoredUnderDerivedKey() {
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();

    KadContent content = ChannelDht.buildRecordContent(secret, randomRecordContent(), now);

    assertThat(content.verify()).isTrue();
    assertThat(content.getId()).isEqualTo(ChannelDht.rendezvousKademliaId(secret, now));
    assertThat(content.getPubkey()).isEqualTo(ChannelDht.deriveRecordNodeId(secret).exportPublic());
  }

  // --- Validation ---

  @Test
  public void isValidRecord_acceptsFreshSignedFixedSizeRecord() {
    long now = System.currentTimeMillis();
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), now);

    assertThat(ChannelDht.isValidRecord(content, now)).isTrue();
  }

  @Test
  public void isValidRecord_rejectsWrongSize() {
    // Correctly derived key but content that is not the fixed bucket size.
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();
    NodeId recordNodeId = ChannelDht.deriveRecordNodeId(secret);
    KadContent content = new KadContent(now, recordNodeId.exportPublic(), randomBytes(100));
    content.signWith(recordNodeId);

    assertThat(ChannelDht.isValidRecord(content, now)).isFalse();
  }

  @Test
  public void isValidRecord_rejectsTamperedContent() {
    long now = System.currentTimeMillis();
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), now);
    // Flip a byte after signing → signature no longer verifies.
    content.getContent()[0] ^= 0xFF;

    assertThat(ChannelDht.isValidRecord(content, now)).isFalse();
  }

  @Test
  public void isValidRecord_rejectsRecordOlderThanTtl() {
    long now = System.currentTimeMillis();
    long published = now - ChannelDht.MAX_RECORD_AGE_MS - 1000;
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), published);

    assertThat(ChannelDht.isValidRecord(content, now)).isFalse();
  }

  @Test
  public void isValidRecord_rejectsRecordTooFarInFuture() {
    long now = System.currentTimeMillis();
    long published = now + ChannelDht.MAX_FUTURE_SKEW_MS + 60_000;
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), published);

    assertThat(ChannelDht.isValidRecord(content, now))
        .as("a future-dated record must not win newest-wins")
        .isFalse();
  }

  // --- Result selection (newest-wins, foreign-record rejection) ---

  @Test
  public void extractNewest_picksNewestValidRecord() {
    byte[] secret = randomChannelSecret();
    // Fixed timestamp safely away from midnight UTC (~22:13 UTC): both records share the same UTC
    // day so they live under the same rotated key, and the explicit nowMs keeps them within TTL —
    // avoids a day-boundary flake from the real wall clock.
    long now = 1_700_000_000_000L;
    KademliaId key = ChannelDht.rendezvousKademliaId(secret, now);

    KadContent older = ChannelDht.buildRecordContent(secret, randomRecordContent(), now - 10_000);
    KadContent newer = ChannelDht.buildRecordContent(secret, randomRecordContent(), now);
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
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), now);

    assertThat(ChannelDht.extractNewest(List.of(foreign), key, now)).isNull();
  }

  @Test
  public void extractNewest_returnsNullForEmptyResults() {
    long now = System.currentTimeMillis();
    KademliaId key = ChannelDht.rendezvousKademliaId(randomChannelSecret(), now);

    assertThat(ChannelDht.extractNewest(List.of(), key, now)).isNull();
  }
}
