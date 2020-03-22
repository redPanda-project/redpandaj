package im.redpanda.kademlia;



import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;

import java.math.BigInteger;
import java.util.Comparator;

/**
 *  A Comparator to compare 2 Peers to a given key, modified for peers, original from Joshua Kissoon github:
 *  https://github.com/JoshuaKissoon/Kademlia
 */
public class PeerComparator implements Comparator<Peer> {


    private final BigInteger key;

    /**
     * @param key The NodeId relative to which the distance should be measured.
     */
    public PeerComparator(KademliaId key) {
        this.key = key.getInt();
    }

    /**
     * Compare two objects which must both be of type <code>Peer</code>
     * and determine which is closest to the identifier specified in the
     * constructor.
     *
     * @param p1 Node 1 to compare distance from the key
     * @param p2 Node 2 to compare distance from the key
     */
    @Override
    public int compare(Peer p1, Peer p2) {
        BigInteger b1 = p1.getKademliaId().getInt();
        BigInteger b2 = p2.getKademliaId().getInt();

        b1 = b1.xor(key);
        b2 = b2.xor(key);

        return b1.abs().compareTo(b2.abs());
    }


}
