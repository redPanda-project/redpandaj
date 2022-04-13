package im.redpanda.flaschenpost;


import im.redpanda.core.KademliaId;
import im.redpanda.core.Server;
import im.redpanda.core.ServerContext;

/**
 * This class represents the basic information for a Flaschenpost and will be extended by a GarlicMessage.
 * A Flaschenpost contains a KademliaId as destination, a random Integer as FP_ID and a timestamp for eviction process.
 */
public abstract class Flaschenpost extends GMContent {

    /**
     * KademliaId will only be used at each Peer and will not be transmitted, since the id can should be calculated
     * from the public key of the GarlicMessage.
     */
    protected KademliaId destination;
    /**
     * This is the Flaschenpost Id and should be a random Integer.
     */
    protected int id;
    /**
     * The timestamp represents the time where this message was created locally and will only be used for the eviction
     * process of the FPStoreManager.
     */
    protected long timestamp;

    protected final ServerContext serverContext;

    /**
     * Creates a new Flaschenpost with the given destination. The Id is a new random integer and the timestamp is
     * the current time.
     *
     * @param destination
     */
    public Flaschenpost(ServerContext serverContext, KademliaId destination) {
        this.serverContext = serverContext;
        this.destination = destination;
        this.id = Server.secureRandom.nextInt();
        this.timestamp = System.currentTimeMillis();
    }

    public Flaschenpost(ServerContext serverContext) {
        this.serverContext = serverContext;
    }

    public int getId() {
        return id;
    }

    public boolean isTargetedToUs() {
        return (destination.equals(serverContext.getNonce()));
    }

    public KademliaId getDestination() {
        return destination;
    }
}
