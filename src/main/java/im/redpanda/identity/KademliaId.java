/**
 * @author Joshua Kissoon
 * @created 20140215
 * @desc Represents a Kademlia Node ID
 */
package im.redpanda.identity;

import im.redpanda.identity.crypt.Base58;
import im.redpanda.identity.crypt.Utils;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;

public class KademliaId {

  public static final int ID_LENGTH = 160;
  public static final int ID_LENGTH_BYTES = ID_LENGTH / 8;
  private final byte[] keyBytes;

  /**
   * Construct the NodeId from some string
   *
   * @param data The user generated key string
   */
  public KademliaId(String data) {
    keyBytes = data.getBytes();
    if (keyBytes.length != ID_LENGTH / 8) {
      throw new IllegalArgumentException(
          "Specified Data need to be "
              + (ID_LENGTH / 8)
              + " characters long. Byte len is: "
              + keyBytes.length);
    }
  }

  /** Generate a random key */
  public KademliaId() {
    keyBytes = new byte[ID_LENGTH / 8];
    new SecureRandom().nextBytes(keyBytes);
  }

  /**
   * Generate the NodeId from a given byte[]
   *
   * @param bytes
   */
  public KademliaId(byte[] bytes) {
    if (bytes.length != ID_LENGTH / 8) {
      throw new IllegalArgumentException(
          "Specified Data need to be "
              + (ID_LENGTH / 8)
              + " characters long. Data Given: '"
              + new String(bytes)
              + "'");
    }
    this.keyBytes = bytes;
  }

  public static KademliaId fromFirstBytes(byte[] bytes) {

    ByteBuffer wrap = ByteBuffer.wrap(bytes);

    byte[] bytesToUse = new byte[ID_LENGTH / 8];

    wrap.get(bytesToUse);

    return new KademliaId(bytesToUse);
  }

  public byte[] getBytes() {
    return this.keyBytes;
  }

  /**
   * @return The BigInteger representation of the key
   */
  public BigInteger getInt() {
    return new BigInteger(1, this.getBytes());
  }

  /**
   * Compares a KademliaId to this KademliaId.
   *
   * <p>This compares the full 160-bit id, byte for byte. It used to compare {@link #hashCode()}
   * instead — i.e. a 32-bit {@link Arrays#hashCode(byte[])} digest of the 20 id bytes — so any two
   * ids colliding on that digest were considered equal. That is a ~2^32 search, which is seconds of
   * offline grinding, and it defeated every identity check built on this method: the handshake
   * public-key/id binding ({@code ConnectionHandler}, {@code ConnectionReaderThread}), {@code
   * NodeId.setKeys}'s keypair check, the {@code ChannelDht} anti-smuggling filter on DHT answers,
   * and {@code isForUs}/next-hop checks in the flaschenpost layer. It also silently conflated two
   * unrelated peers or DHT records into one slot in every map keyed by a KademliaId ({@code
   * PeerList.peerHashMap}, {@code KadStoreManager.entries}, {@code
   * KademliaSearchJob.kademliaIdSearchBlacklist}, the MapDB node stores, and — via {@link
   * NodeId#equals} / {@code Node.equals} — the JGraphT routing graph).
   *
   * <p>{@link #hashCode()} is deliberately left unchanged: equal ids still produce equal hashes, so
   * the equals/hashCode contract holds, and no hash-based container (including the persisted MapDB
   * {@code nodecache*.mapdb} and the persisted node graph) changes its bucket layout — colliding
   * entries simply stop being merged.
   *
   * @param o The KademliaId to compare to this KademliaId
   * @return boolean Whether the 2 KademliaIds are equal
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (o == null) {
      return false;
    }

    if (o instanceof KademliaId nid) {
      return Arrays.equals(this.keyBytes, nid.keyBytes);
    }
    // Pre-existing, deliberate strictness: a cross-type comparison is always a programming error
    // here, and failing loudly beats silently returning false. It does technically deviate from the
    // Object.equals contract, but nothing can reach it today — NodeId.equals and Node.equals both
    // type-guard before delegating, and every container keyed by a KademliaId is homogeneously
    // typed (audited for the H1 fix). Turning it into `return false` would be an orthogonal
    // behaviour change that could mask real type confusion, so it is left as is.
    throw new RuntimeException("do not compare KademliaId to other objects!");
  }

  /**
   * Note: this is a 32-bit digest of the id and must therefore never be used as an equality or
   * identity test on its own — see {@link #equals(Object)}. It is kept as-is so equal ids keep
   * equal hashes and existing hash-based containers need no rehash/migration.
   */
  @Override
  public int hashCode() {
    int hash = 7;
    hash = 83 * hash + Arrays.hashCode(this.keyBytes);
    return hash;
  }

  /**
   * Checks the distance between this and another NodeId
   *
   * @param nid
   * @return The distance of this NodeId from the given NodeId
   */
  public KademliaId xor(KademliaId nid) {
    byte[] result = new byte[ID_LENGTH / 8];
    byte[] nidBytes = nid.getBytes();

    for (int i = 0; i < ID_LENGTH / 8; i++) {
      result[i] = (byte) (this.keyBytes[i] ^ nidBytes[i]);
    }

    KademliaId resNid = new KademliaId(result);

    return resNid;
  }

  /**
   * Counts the number of leading 0's in this NodeId
   *
   * @return Integer The number of leading 0's
   */
  public int getFirstSetBitIndex() {
    int prefixLength = 0;

    for (byte b : this.keyBytes) {
      if (b == 0) {
        prefixLength += 8;
      } else {
        /* If the byte is not 0, we need to count how many MSBs are 0 */
        int count = 0;
        for (int i = 7; i >= 0; i--) {
          boolean a = (b & (1 << i)) == 0;
          if (a) {
            count++;
          } else {
            break; // Reset the count if we encounter a non-zero number
          }
        }

        /* Add the count of MSB 0s to the prefix length */
        prefixLength += count;

        /* Break here since we've now covered the MSB 0s */
        break;
      }
    }
    return prefixLength;
  }

  /**
   * Gets the distance from this NodeId to another NodeId
   *
   * @param to
   * @return Integer The distance
   */
  public int getDistance(KademliaId to) {
    /**
     * Compute the xor of this and to Get the index i of the first set bit of the xor returned
     * NodeId The distance between them is ID_LENGTH - i
     */
    return ID_LENGTH - this.xor(to).getFirstSetBitIndex();
  }

  public String hexRepresentation() {
    /* Returns the hex format of this NodeId */
    return Utils.bytesToHexString(keyBytes);
  }

  @Override
  public String toString() {
    return Base58.encode(keyBytes).substring(0, 10);
  }

  public static KademliaId fromBuffer(ByteBuffer buffer) {
    byte[] bytes = new byte[ID_LENGTH_BYTES];
    buffer.get(bytes);
    return new KademliaId(bytes);
  }
}
