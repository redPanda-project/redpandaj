package im.redpanda.flaschenpost;


import im.redpanda.core.Log;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;


@ThreadSafe
public class FPStoreManager {

    /**
     * This Hashmap maps a FP_ID to a timestamp. The timestamp represents the time where the FP_ID was added
     */
    private static final HashMap<Integer, Flaschenpost> entries = new HashMap<>();
    private static final ReentrantLock lock = new ReentrantLock();


    /**
     * we put a {@link Flaschenpost} into this set to remember that we already broadcasted that {@link Flaschenpost} to other peers
     * we do not have to verify the integrity of the content, since the ACK message is encrypted and we will only obtain
     * the ack_id if we do not modify the content. Since a Peer does not know if we are the originator of the message,
     * he can not modify the data without punishment, if we reduce the scoring of that peer if we do not obtain an ACK_ID.
     *
     * @param fp {@link Flaschenpost} to be stored.
     * @return true if the object was already stored.
     */
    public boolean put(Flaschenpost fp) {
        lock.lock();
        try {
            Flaschenpost put = entries.put(fp.getId(), fp);
            return put == null ? false : true;
        } finally {
            lock.unlock();
        }
    }

    //todo, we have to create a job for eviction of old entries...

}
