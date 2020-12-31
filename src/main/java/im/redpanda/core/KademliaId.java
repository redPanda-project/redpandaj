/**
 * @author Joshua Kissoon
 * @created 20140215
 * @desc Represents a Kademlia Node ID
 */
package im.redpanda.core;

import im.redpanda.crypt.AddressFormatException;
import im.redpanda.crypt.Base58;
import im.redpanda.crypt.Utils;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.BitSet;

public class KademliaId implements Serializable {

    public final transient static int ID_LENGTH = 160;
    public final transient static int ID_LENGTH_BYTES = (ID_LENGTH / 8);
    private byte[] keyBytes;
    private int nodeDistance = -1;
//    private BigInteger bigInt;

    /**
     * Construct the NodeId from some string
     *
     * @param data The user generated key string
     */
    public KademliaId(String data) {
        keyBytes = data.getBytes();
        if (keyBytes.length != ID_LENGTH / 8) {
            throw new IllegalArgumentException("Specified Data need to be " + (ID_LENGTH / 8) + " characters long. Byte len is: " + keyBytes.length);
        }
    }

    /**
     * Generate a random key
     */
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
            throw new IllegalArgumentException("Specified Data need to be " + (ID_LENGTH / 8) + " characters long. Data Given: '" + new String(bytes) + "'");
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
//        //lets cache that BigInt for performance reasons
//        if (bigInt == null) {
//            bigInt = new BigInteger(1, this.getBytes());
//        }
//
//        return bigInt;
        return new BigInteger(1, this.getBytes());
    }

    /**
     * Compares a KademliaId to this KademliaId
     *
     * @param o The KademliaId to compare to this KademliaId
     * @return boolean Whether the 2 KademliaIds are equal
     */
    @Override
    public boolean equals(Object o) {

        //check if same instance!
        if (o == this) {
            return true;
        }

        if (o == null) {
            return false;
        }

        if (o instanceof KademliaId) {
            KademliaId nid = (KademliaId) o;
            return this.hashCode() == nid.hashCode();
//            return Arrays.equals(this.getBytes(), nid.getBytes());
        }
        throw new RuntimeException("do not compare KademliaId to other objects!");
//        return false;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 83 * hash + Arrays.hashCode(this.keyBytes);
//        throw new RuntimeException("asdf!!!");
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
     * Generates a NodeId that is some distance away from this NodeId
     *
     * @param distance in number of bits
     * @return NodeId The newly generated NodeId
     */
    public KademliaId generateNodeIdByDistance(int distance) {
        byte[] result = new byte[ID_LENGTH / 8];

        /* Since distance = ID_LENGTH - prefixLength, we need to fill that amount with 0's */
        int numByteZeroes = (ID_LENGTH - distance) / 8;
        int numBitZeroes = 8 - (distance % 8);

        /* Filling byte zeroes */
        for (int i = 0; i < numByteZeroes; i++) {
            result[i] = 0;
        }

        /* Filling bit zeroes */
        BitSet bits = new BitSet(8);
        bits.set(0, 8);

        for (int i = 0; i < numBitZeroes; i++) {
            /* Shift 1 zero into the start of the value */
            bits.clear(i);
        }
        bits.flip(0, 8);        // Flip the bits since they're in reverse order
        result[numByteZeroes] = (byte) bits.toByteArray()[0];

        /* Set the remaining bytes to Maximum value */
        for (int i = numByteZeroes + 1; i < result.length; i++) {
            result[i] = Byte.MAX_VALUE;
        }

        return this.xor(new KademliaId(result));
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
                        break;   // Reset the count if we encounter a non-zero number
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
         * Compute the xor of this and to
         * Get the index i of the first set bit of the xor returned NodeId
         * The distance between them is ID_LENGTH - i
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

    public static KademliaId fromBase58(String base58String) throws AddressFormatException {
        return new KademliaId(Base58.decode(base58String));
    }

    /**
     * get the distance to Test.NONCE with caching
     *
     * @return distance to Test.NONCE
     */
    public int getDistanceToUs() {
        if (nodeDistance != -1) {
            return nodeDistance;
        }

        nodeDistance = getDistance(Server.NONCE);
        return nodeDistance;
    }

    public static KademliaId fromBuffer(ByteBuffer buffer) {
        byte[] bytes = new byte[ID_LENGTH_BYTES];
        buffer.get(bytes);
        return new KademliaId(bytes);
    }
}
