package im.redpanda.flaschenpost;

import im.redpanda.core.Log;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class GMStoreManager {

  public static final long REMOVE_AFTER_MILLISECONDS = 1000L * 60L * 5L;
  private static final HashMap<GarlicMessage, Long> entries = new HashMap<>();

  /** MS04: seen Flaschenpost v2 packet ids (dedup and loop protection), same 5-minute window. */
  private static final HashMap<Integer, Long> v2Entries = new HashMap<>();

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

  /**
   * Marks a Flaschenpost v2 {@code packet_id} as seen.
   *
   * @return {@code true} if the packet id was already present (duplicate — drop the packet)
   */
  public static boolean putV2(int packetId) {
    lock.lock();
    try {
      Long put = v2Entries.put(packetId, System.currentTimeMillis());
      return put != null;
    } finally {
      lock.unlock();
    }
  }

  public static void cleanUp() {
    long currentTimeMillis = System.currentTimeMillis();
    lock.lock();
    try {

      removeExpired(entries, currentTimeMillis);
      removeExpired(v2Entries, currentTimeMillis);

      Log.put(
          "remaining entries in GMStoreManager after cleanup: %d v2: %d"
              .formatted(entries.size(), v2Entries.size()),
          0);
    } finally {
      lock.unlock();
    }
  }

  private static void removeExpired(HashMap<?, Long> map, long currentTimeMillis) {
    Iterator<? extends Map.Entry<?, Long>> iterator = map.entrySet().iterator();
    while (iterator.hasNext()) {
      if (currentTimeMillis - iterator.next().getValue() > REMOVE_AFTER_MILLISECONDS) {
        iterator.remove();
      }
    }
  }
}
