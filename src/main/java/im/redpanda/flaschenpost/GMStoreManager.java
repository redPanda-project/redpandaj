package im.redpanda.flaschenpost;


import im.redpanda.core.Log;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;


@ThreadSafe
public class GMStoreManager {

    public static final long REMOVE_AFTER_MILLISECONDS = 1000L * 60L * 5L;
    private static final HashMap<GarlicMessage, Long> entries = new HashMap<>();
    private static final ReentrantLock lock = new ReentrantLock();

    public static boolean put(GarlicMessage gm) {
        lock.lock();
        try {
            Long put = entries.put(gm, System.currentTimeMillis());
            return put != null;
        } finally {
            lock.unlock();
        }
    }

    public static void cleanUp() {
        long currentTimeMillis = System.currentTimeMillis();
        lock.lock();
        try {

            Iterator<Map.Entry<GarlicMessage, Long>> iterator = entries.entrySet().iterator();
            while (iterator.hasNext()) {
                if (currentTimeMillis - iterator.next().getValue() > REMOVE_AFTER_MILLISECONDS) {
                    iterator.remove();
                }
            }

            Log.put(String.format("remaining entries in GMStoreManager after cleanup: %d", entries.size()), 0);
        } finally {
            lock.unlock();
        }
    }

}
