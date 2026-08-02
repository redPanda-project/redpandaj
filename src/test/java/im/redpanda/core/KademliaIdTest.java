package im.redpanda.core;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.security.Security;
import java.util.Arrays;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

class KademliaIdTest {

  static {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
  }

  @Test
  void equals() {

    NodeId nodeId = new NodeId();

    KademliaId kademliaId = nodeId.getKademliaId();

    KademliaId clonedByBytes = KademliaId.fromBuffer(ByteBuffer.wrap(kademliaId.getBytes()));

    assertEquals(kademliaId, clonedByBytes);
  }

  /**
   * Two distinct 20-byte ids that collide under {@link Arrays#hashCode(byte[])}.
   *
   * <p>How they were produced (no search needed): {@code Arrays.hashCode} is the polynomial {@code
   * h = 31*h + b_i} over the bytes, so the contribution of two adjacent positions {@code i, i+1} to
   * the final value differs by {@code 31^k * (31*d_i + d_i+1)}, where {@code d} is the per-byte
   * difference between the two arrays. Choosing {@code d_i = +1} and {@code d_i+1 = -31} makes that
   * factor exactly zero, so the two hashes are identical — no modular arithmetic and no brute force
   * involved. Concretely: {@code {0, 31, 0...}} vs. {@code {1, 0, 0...}}.
   *
   * <p>An attacker grinding a keypair whose derived id collides with a target id faces the same
   * 2^32 problem, which is seconds of offline work — hence H1.
   */
  private static KademliaId collidingIdA() {
    byte[] bytes = new byte[KademliaId.ID_LENGTH_BYTES];
    bytes[0] = 0;
    bytes[1] = 31;
    return new KademliaId(bytes);
  }

  private static KademliaId collidingIdB() {
    byte[] bytes = new byte[KademliaId.ID_LENGTH_BYTES];
    bytes[0] = 1;
    bytes[1] = 0;
    return new KademliaId(bytes);
  }

  /**
   * Guards the fixture itself: if this ever stops holding, the regression tests below would
   * silently stop testing anything.
   */
  @Test
  void collidingFixture_reallyCollidesOn32Bits() {
    KademliaId a = collidingIdA();
    KademliaId b = collidingIdB();

    assertFalse(Arrays.equals(a.getBytes(), b.getBytes()), "fixture must be two DIFFERENT ids");
    assertEquals(
        Arrays.hashCode(a.getBytes()),
        Arrays.hashCode(b.getBytes()),
        "fixture must collide under Arrays.hashCode");
    // and therefore also under KademliaId.hashCode(), which is a pure function of it
    assertEquals(a.hashCode(), b.hashCode());
  }

  /**
   * H1 regression: {@code equals()} used to return {@code this.hashCode() == nid.hashCode()} — a
   * 32-bit digest of the 20 id bytes — so two distinct ids colliding on that digest compared equal.
   * Every identity check in the codebase is built on this: the handshake public-key/id binding
   * ({@code ConnectionHandler}, {@code ConnectionReaderThread}), {@code NodeId.setKeys}, the {@code
   * ChannelDht} anti-smuggling filter, {@code Flaschenpost.isForUs}, the next-hop check in {@code
   * GarlicRouter}. Equality must compare the full 160-bit id.
   */
  @Test
  void equals_distinctIdsCollidingOn32BitHash_areNotEqual() {
    KademliaId a = collidingIdA();
    KademliaId b = collidingIdB();

    assertNotEquals(a, b);
    assertNotEquals(b, a);
  }

  /**
   * The equals/hashCode contract must still hold after the change: equal ids keep equal hashes.
   * That is what lets {@code hashCode()} stay untouched — no hash-based container (including the
   * persisted MapDB node store and the serialized JGraphT graph) needs a rehash or a migration.
   */
  @Test
  void hashCode_isStillConsistentWithEquals() {
    KademliaId id = new KademliaId();
    KademliaId sameBytes = new KademliaId(Arrays.copyOf(id.getBytes(), id.getBytes().length));

    assertEquals(id, sameBytes);
    assertEquals(id.hashCode(), sameBytes.hashCode());
  }

  /**
   * The concrete consequence for every map keyed by a KademliaId ({@code PeerList.peerHashMap},
   * {@code KadStoreManager.entries}, {@code KademliaSearchJob.kademliaIdSearchBlacklist}, the MapDB
   * node stores, and via {@code NodeId}/{@code Node} the routing graph): two colliding ids used to
   * land in the same slot, so the second {@code put} silently overwrote the first and a lookup with
   * one id returned the other's value. They must now be two independent entries — deliberately in
   * the same hash bucket, so this also exercises the bucket's equals-based collision handling.
   */
  @Test
  void hashMapKeyedByKademliaId_keepsCollidingIdsApart() {
    KademliaId a = collidingIdA();
    KademliaId b = collidingIdB();

    HashMap<KademliaId, String> map = new HashMap<>();
    map.put(a, "valueA");
    map.put(b, "valueB");

    assertEquals(2, map.size());
    assertEquals("valueA", map.get(a));
    assertEquals("valueB", map.get(b));
  }

  /**
   * H1 propagates through {@code NodeId.equals}, which delegates to {@code KademliaId.equals} — and
   * that in turn is what {@code Node.equals} (the JGraphT vertex type) uses. Two NodeIds built from
   * colliding KademliaIds must be distinct identities.
   */
  @Test
  void nodeIdEquals_inheritsTheStricterComparison() {
    NodeId a = new NodeId(collidingIdA());
    NodeId b = new NodeId(collidingIdB());

    assertNotEquals(a, b);
    // still consistent: the delegated hashCode collides, which is allowed and is exactly the case
    // hash containers must resolve via equals
    assertEquals(a.hashCode(), b.hashCode());
  }
}
