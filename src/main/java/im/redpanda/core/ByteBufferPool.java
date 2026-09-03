package im.redpanda.core;

import im.redpanda.App;
import im.redpanda.ops.Log;
import io.sentry.Breadcrumb;
import io.sentry.Sentry;
import io.sentry.SentryLevel;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
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

  /**
   * System property enabling the return-stack tracing below (L3, bug hunt 2026-07-26).
   *
   * <p>The map is a pure debug aid: its only consumer is the Sentry message in {@link
   * #borrowObject(Integer)}, which names the caller that last returned a buffer that came back out
   * of the pool in an invalid state. Capturing and formatting a full stack trace on <em>every</em>
   * successful {@code returnObject()} is a hot-path cost for a diagnostic that is almost never
   * read, so it is opt-in and off by default. The invalid-return path itself keeps reporting
   * unconditionally — that branch already captures its own stack trace and does not depend on this
   * map.
   */
  static final String TRACE_RETURNS_PROPERTY = "redpanda.bytebufferpool.traceReturns";

  private static volatile boolean traceReturns = Boolean.getBoolean(TRACE_RETURNS_PROPERTY);

  /**
   * Identity map (buffer equality is content-based and the content mutates, so {@link
   * IdentityHashMap} is required) from a pooled buffer to the stack trace of its last return.
   *
   * <p>Entries are dropped again when the pool destroys the buffer — commons-pool's evictor
   * destroys idle buffers after 30 s, and the entries used to stay strongly reachable for the whole
   * process lifetime, so the map (and every multi-kilobyte trace string in it) grew without bound
   * under bursty load. Accessed from every reader/writer thread, hence synchronized.
   */
  private static final Map<ByteBuffer, String> byteBufferToStacktrace =
      Collections.synchronizedMap(new IdentityHashMap<>());

  private ByteBufferPool() {
    // Hide implicit public constructor
  }

  /** Test hook: toggles the return-stack tracing and returns the previous setting. */
  static boolean setTraceReturnsForTest(boolean enabled) {
    boolean previous = traceReturns;
    traceReturns = enabled;
    return previous;
  }

  /** Test hook: whether a return stack trace is currently held for this exact buffer instance. */
  static boolean isTraced(ByteBuffer byteBuffer) {
    return byteBufferToStacktrace.containsKey(byteBuffer);
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
            // TD021: this runs on every buffer allocation, so it must stay below the
            // effective log level (slf4j binds to logback here, whose default root level
            // is DEBUG because no logback.xml is shipped). The guard also keeps the
            // listAllObjects() walk and the StringBuilder off the hot path entirely
            // unless trace is explicitly enabled.
            if (log.isTraceEnabled()) {
              log.trace(
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

              log.trace("\n\nList of Pool: \n{}\n\n", out);
            }

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
          public void destroyObject(Integer key, PooledObject<ByteBuffer> p) throws Exception {
            // The recorded stack trace must not outlive the buffer it describes: the evictor
            // destroys idle buffers after 30 s, and without this the entry (and its multi-kilobyte
            // trace string) stayed strongly reachable forever.
            byteBufferToStacktrace.remove(p.getObject());
            super.destroyObject(key, p);
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
      String stack =
          traceReturns
              ? byteBufferToStacktrace.get(byteBuffer)
              : "not recorded (-D" + TRACE_RETURNS_PROPERTY + "=true to enable)";
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
      if (traceReturns) {
        StringBuilder out = new StringBuilder();
        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
          out.append(e.toString()).append("\n");
        }
        byteBufferToStacktrace.put(byteBuffer, out.toString());
      }
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
