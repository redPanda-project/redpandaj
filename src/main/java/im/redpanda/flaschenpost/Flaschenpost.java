package im.redpanda.flaschenpost;


import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.core.Server;

/**
 * This class represents the basic information for a Flaschenpost and will be extended by a GarlicMessage.
 * A Flaschenpost contains a KademliaId as destination, a random Integer as FP_ID and a timestamp for eviction process.
 */
public abstract class Flaschenpost extends GMContent {

    /**
     * KademliaId will only be used at each Peer and will not be transmitted, since the id can should be calculated
     * from the public key of the GarlicMessage.
     */
    private KademliaId destination;
    /**
     * This is the FP_ID and should be a random Interger.
     */
    private int id;
    /**
     * The timestamp represents the time where this message was created locally and will only be used for the eviction
     * process of the FPStoreManager.
     */
    private long timestamp;
    /**
     * Byte representation of the encrypted content. This should be created after the encryption process and is used
     * to transmit the Flaschenpost over the wire.
     */
    protected byte[] content;

    public int getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static void put(KademliaId destination, byte[] content) {


        //let us get the closest node in our peerlist to the destination

        // we only want to find peers which are closer to destination than we are
        int nearestDistance = destination.getDistance(Server.NONCE);
        Peer nearestPeer = null;

        System.out.println("our distance: " + nearestDistance);

        PeerList.getReadWriteLock().readLock().lock();
        try {
            for (Peer p : PeerList.getPeerArrayList()) {

                if (p.getNodeId() == null || !p.isIntegrated()) {
                    continue;
                }

                int distance = p.getKademliaId().getDistance(destination);
                System.out.println("distance: " + distance);
                if (distance < nearestDistance) {
                    nearestDistance = distance;
                    nearestPeer = p;
                }

            }


            if (nearestPeer == null) {
                System.out.println("we are the closest peer!");
            } else {
                System.out.println("found peer with distance: " + nearestDistance + " peer: " + nearestPeer.getNodeId());
            }


        } finally {
            PeerList.getReadWriteLock().readLock().unlock();
        }


    }

}
