package im.redpanda.core;

import im.redpanda.App;
import io.sentry.Breadcrumb;
import io.sentry.Sentry;
import io.sentry.SentryLevel;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObjectInfo;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class ByteBufferPool {

  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(ByteBufferPool.class);
  private static GenericKeyedObjectPool<Integer, ByteBuffer> pool;
  private static final Map<ByteBuffer, String> byteBufferToStacktrace = new IdentityHashMap<>();

  private ByteBufferPool() {
    // Hide implicit public constructor
  }

  public static void init() {
    if (pool != null) {
      return;
    }

    BaseKeyedPooledObjectFactory<Integer, ByteBuffer> pooledObjectFactory =
        new BaseKeyedPooledObjectFactory<>() {
          @Override
          public ByteBuffer create(Integer size) throws Exception {

            ByteBuffer allocate = ByteBuffer.allocate(size);

            if (Runtime.getRuntime().freeMemory() < 1024 * 1024 * 200) {
              pool.setMaxTotalPerKey(200);
            } else {
              pool.setMaxTotalPerKey(400);
            }
            log.info(
                "Generating new ByteBuffer for pool. Free memory (MB): {} Idle: {} Active: {} Waiters: {}",
                (Runtime.getRuntime().freeMemory() / 1024. / 1024.),
                pool.getNumIdle(),
                pool.getNumActive(),
                pool.getNumWaiters());

            Map<String, List<DefaultPooledObjectInfo>> stringListMap = pool.listAllObjects();

            StringBuilder out = new StringBuilder();

            for (Map.Entry<String, List<DefaultPooledObjectInfo>> entry :
                stringListMap.entrySet()) {
              out.append("key: ")
                  .append(entry.getKey())
                  .append(" size: ")
                  .append(entry.getValue().size())
                  .append("\n");
            }

            log.info("\n\nList of Pool: \n{}\n\n", out);

            return allocate;
          }

          @Override
          public void passivateObject(Integer key, PooledObject<ByteBuffer> p) throws Exception {
            ByteBuffer byteBuffer = p.getObject();

            byteBuffer.position(0);
            byteBuffer.limit(byteBuffer.capacity());

            super.passivateObject(key, p);
          }

          @Override
          public boolean validateObject(Integer key, PooledObject<ByteBuffer> p) {
            boolean b = p.getObject().position() == 0;
            if (log.isDebugEnabled()) {
              log.debug("validateObject: {}", b);
            }
            return b;
          }

          @Override
          public PooledObject<ByteBuffer> wrap(ByteBuffer byteBuffer) {
            return new DefaultPooledObject<>(byteBuffer);
          }
        };

    pool = new GenericKeyedObjectPool<>(pooledObjectFactory);
    pool.setMinIdlePerKey(0);
    pool.setMinEvictableIdle(Duration.ofSeconds(30));
    pool.setTimeBetweenEvictionRuns(Duration.ofSeconds(5)); // will only test 3 items
    pool.setNumTestsPerEvictionRun(3);
  }

  public static GenericKeyedObjectPool<Integer, ByteBuffer> getPool() {
    return pool;
  }

  public static ByteBuffer borrowObject(Integer key) {
    key = keyToKey(key);

    ByteBuffer byteBuffer = null;
    try {
      byteBuffer = pool.borrowObject(key);
    } catch (Exception e) {
      e.printStackTrace();
    }

    if (byteBuffer == null) {
      return null;
    }

    while (byteBuffer.position() != 0 || byteBuffer.limit() != byteBuffer.capacity()) {
      String stack = byteBufferToStacktrace.get(byteBuffer);
      Log.sentry("borrowObject found an invalid ByteBuffer: " + byteBuffer + " stack: " + stack);
      try {
        pool.invalidateObject(key, byteBuffer);
        byteBuffer = pool.borrowObject(key);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return byteBuffer;
  }

  /**
   * Returns the ByteBuffer to the pool, the key is calculated from the capacity of the ByteBuffer.
   *
   * @param byteBuffer
   */
  public static void returnObject(ByteBuffer byteBuffer) {

    int key = byteBuffer.capacity();

    key = keyToKey(key);

    if (byteBuffer.position() != 0 || byteBuffer.limit() != byteBuffer.capacity()) {
      try {
        pool.invalidateObject(key, byteBuffer);
      } catch (Exception e) {
        e.printStackTrace();
      }

      StringBuilder out = new StringBuilder();
      for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
        out.append(e.toString()).append("\n");
      }

      if (App.sentryAllowed) {
        Breadcrumb breadcrumb = new Breadcrumb();
        breadcrumb.setCategory("IO");
        breadcrumb.setMessage("bytebuffer: " + byteBuffer);
        breadcrumb.setLevel(SentryLevel.WARNING);
        Sentry.addBreadcrumb(breadcrumb);
        Log.sentry("had to invalidate ByteBuffer: \n" + out);
      }
    } else {
      StringBuilder out = new StringBuilder();
      for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
        out.append(e.toString()).append("\n");
      }
      byteBufferToStacktrace.put(byteBuffer, out.toString());
      pool.returnObject(key, byteBuffer);
    }
  }

  public static int keyToKey(int key) {
    if (key <= 16) {
      key = 16;
    } else if (key <= 1024) {
      key = 1024;
    } else if (key <= 1024 * 1024) {
      key = 1024 * 1024;
    } else if (key <= 10 * 1024 * 1024) {
      key = 10 * 1024 * 1024;
    } else {
      key = 40 * 1024 * 1024;
    }
    return key;
  }
}
