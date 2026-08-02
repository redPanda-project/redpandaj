package im.redpanda.outbound;

import static org.assertj.core.api.Assertions.assertThat;

import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import im.redpanda.crypt.Utils;
import im.redpanda.kademlia.KadContent;
import java.security.SecureRandom;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * T43: tests for the channel-rendezvous DHT primitives — deterministic record-key derivation from
 * the channel secret, fixed-size padded (opaque) records, TTL / signature / size validation and
 * newest-wins result selection.
 */
class ChannelDhtTest {

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
  void deriveRecordNodeId_isDeterministic() {
    byte[] secret = randomChannelSecret();

    NodeId first = ChannelDht.deriveRecordNodeId(secret);
    NodeId second = ChannelDht.deriveRecordNodeId(secret.clone());

    assertThat(first.exportPublic()).isEqualTo(second.exportPublic());
    assertThat(first.getKademliaId()).isEqualTo(second.getKademliaId());
  }

  @Test
  void deriveRecordNodeId_differsPerChannel() {
    NodeId first = ChannelDht.deriveRecordNodeId(randomChannelSecret());
    NodeId second = ChannelDht.deriveRecordNodeId(randomChannelSecret());

    assertThat(first.exportPublic()).isNotEqualTo(second.exportPublic());
  }

  @Test
  void rendezvousKademliaId_sameForEveryParticipant() {
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();

    assertThat(ChannelDht.rendezvousKademliaId(secret, now))
        .isEqualTo(ChannelDht.rendezvousKademliaId(secret.clone(), now));
  }

  @Test
  void rendezvousKademliaId_isDomainSeparatedFromOhAnnounce() {
    // Feeding the same 32 bytes as an oh_id vs a channel secret must land in different namespaces:
    // the channel record key is derived through a distinct domain tag.
    byte[] shared = randomChannelSecret();
    long now = System.currentTimeMillis();

    assertThat(ChannelDht.rendezvousKademliaId(shared, now))
        .isNotEqualTo(OhDht.announceKademliaId(shared, now));
  }

  // --- Record building (padding, signature, self-certifying key) ---

  @Test
  void buildRecordContent_recordsHaveConstantSize() {
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
  void buildRecordContent_rejectsWrongSizeContent() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                ChannelDht.buildRecordContent(
                    randomChannelSecret(), randomBytes(100), System.currentTimeMillis()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRecordContent_isSignedAndStoredUnderDerivedKey() {
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();

    KadContent content = ChannelDht.buildRecordContent(secret, randomRecordContent(), now);

    assertThat(content.verify()).isTrue();
    assertThat(content.getId()).isEqualTo(ChannelDht.rendezvousKademliaId(secret, now));
    assertThat(content.getPubkey()).isEqualTo(ChannelDht.deriveRecordNodeId(secret).exportPublic());
  }

  @Test
  void buildRecordContent_matchesTheClientCrossCheckVector() {
    // Pins the bucket size and the exact signature the dart light client asserts against
    // (channel_rendezvous_test.dart). Both sides sign the same bytes, so a one-sided change to
    // RECORD_SIZE_BYTES — the kind that silently makes every published record undeliverable —
    // fails here instead of in the field.
    byte[] secret = new byte[32];
    for (int i = 0; i < secret.length; i++) {
      secret[i] = (byte) i;
    }
    byte[] content = new byte[ChannelDht.RECORD_SIZE_BYTES];
    java.util.Arrays.fill(content, (byte) 0xAB);

    KadContent record = ChannelDht.buildRecordContent(secret, content, 1000000000000L);

    assertThat(ChannelDht.RECORD_SIZE_BYTES).isEqualTo(1024);
    assertThat(Utils.bytesToHexString(record.getSignature()))
        .isEqualTo(
            "e649ea68beaedc8e66a11765ec5d4b3fbac2bf58e54815105741fd6007276893"
                + "4838668388001641eac6c21223ec4e195fc590b1c91571ae9c0699932b411600");
  }

  // --- Validation ---

  @Test
  void isValidRecord_acceptsFreshSignedFixedSizeRecord() {
    long now = System.currentTimeMillis();
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), now);

    assertThat(ChannelDht.isValidRecord(content, now)).isTrue();
  }

  @Test
  void isValidRecord_rejectsWrongSize() {
    // Correctly derived key but content that is not the fixed bucket size.
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();
    NodeId recordNodeId = ChannelDht.deriveRecordNodeId(secret);
    KadContent content = new KadContent(now, recordNodeId.exportPublic(), randomBytes(100));
    content.signWith(recordNodeId);

    assertThat(ChannelDht.isValidRecord(content, now)).isFalse();
  }

  @Test
  void isValidRecord_rejectsTamperedContent() {
    long now = System.currentTimeMillis();
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), now);
    // Flip a byte after signing → signature no longer verifies.
    content.getContent()[0] ^= 0xFF;

    assertThat(ChannelDht.isValidRecord(content, now)).isFalse();
  }

  @Test
  void isValidRecord_rejectsRecordOlderThanTtl() {
    long now = System.currentTimeMillis();
    long published = now - ChannelDht.MAX_RECORD_AGE_MS - 1000;
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), published);

    assertThat(ChannelDht.isValidRecord(content, now)).isFalse();
  }

  @Test
  void isValidRecord_rejectsRecordTooFarInFuture() {
    long now = System.currentTimeMillis();
    long published = now + ChannelDht.MAX_FUTURE_SKEW_MS + 60_000;
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), published);

    assertThat(ChannelDht.isValidRecord(content, now))
        .as("a future-dated record must not win newest-wins")
        .isFalse();
  }

  // --- Validation reasons (TD022: each failure cause maps to its own distinct outcome, so the
  // record-store drop log can tell a protocol-size skew apart from routine garbage) ---

  @Test
  void validateRecord_reportsValidForFreshSignedFixedSizeRecord() {
    long now = System.currentTimeMillis();
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), now);

    assertThat(ChannelDht.validateRecord(content, now))
        .isEqualTo(ChannelDht.RecordValidation.VALID);
  }

  @Test
  void validateRecord_reportsMissingContent() {
    long now = System.currentTimeMillis();

    assertThat(ChannelDht.validateRecord(null, now))
        .isEqualTo(ChannelDht.RecordValidation.MISSING_CONTENT);

    KadContent noBytes =
        new KadContent(
            now,
            ChannelDht.deriveRecordNodeId(randomChannelSecret()).exportPublic(),
            null,
            new byte[64]);
    assertThat(ChannelDht.validateRecord(noBytes, now))
        .isEqualTo(ChannelDht.RecordValidation.MISSING_CONTENT);
  }

  @Test
  void validateRecord_reportsWrongSizeEvenWhenCorrectlySigned() {
    // A correctly signed record of the wrong bucket size is exactly the TD022 version-skew case
    // (e.g. a client still publishing the old 512-byte bucket): the size verdict must win over
    // the signature so the drop log can name the deployment error.
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();
    NodeId recordNodeId = ChannelDht.deriveRecordNodeId(secret);
    KadContent content = new KadContent(now, recordNodeId.exportPublic(), randomBytes(512));
    content.signWith(recordNodeId);

    assertThat(ChannelDht.validateRecord(content, now))
        .isEqualTo(ChannelDht.RecordValidation.WRONG_SIZE);
  }

  @Test
  void validateRecord_reportsExpired() {
    long now = System.currentTimeMillis();
    long published = now - ChannelDht.MAX_RECORD_AGE_MS - 1000;
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), published);

    assertThat(ChannelDht.validateRecord(content, now))
        .isEqualTo(ChannelDht.RecordValidation.EXPIRED);
  }

  @Test
  void validateRecord_reportsFutureDated() {
    long now = System.currentTimeMillis();
    long published = now + ChannelDht.MAX_FUTURE_SKEW_MS + 60_000;
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), published);

    assertThat(ChannelDht.validateRecord(content, now))
        .isEqualTo(ChannelDht.RecordValidation.FUTURE_DATED);
  }

  @Test
  void validateRecord_reportsBadSignature() {
    long now = System.currentTimeMillis();
    KadContent content =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), now);
    content.getContent()[0] ^= 0xFF;

    assertThat(ChannelDht.validateRecord(content, now))
        .isEqualTo(ChannelDht.RecordValidation.BAD_SIGNATURE);
  }

  // --- Result selection (newest-wins, foreign-record rejection) ---

  @Test
  void extractNewest_picksNewestValidRecord() {
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
  void extractNewest_rejectsRecordUnderDifferentKey() {
    byte[] secret = randomChannelSecret();
    long now = System.currentTimeMillis();
    KademliaId key = ChannelDht.rendezvousKademliaId(secret, now);

    // A validly signed record for ANOTHER channel must not be served for this key.
    KadContent foreign =
        ChannelDht.buildRecordContent(randomChannelSecret(), randomRecordContent(), now);

    assertThat(ChannelDht.extractNewest(List.of(foreign), key, now)).isNull();
  }

  @Test
  void extractNewest_returnsNullForEmptyResults() {
    long now = System.currentTimeMillis();
    KademliaId key = ChannelDht.rendezvousKademliaId(randomChannelSecret(), now);

    assertThat(ChannelDht.extractNewest(List.of(), key, now)).isNull();
  }
}
