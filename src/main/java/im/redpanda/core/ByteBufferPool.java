package im.redpanda.core;

import im.redpanda.App;
import io.sentry.Breadcrumb;
import io.sentry.Sentry;
import io.sentry.SentryLevel;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObjectInfo;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class ByteBufferPool {


    private static GenericKeyedObjectPool<Integer, ByteBuffer> pool;
    private static final Map<ByteBuffer, String> byteBufferToStacktrace = new IdentityHashMap<>();

    public static void init() {
        if (pool != null) {
            return;
        }

        BaseKeyedPooledObjectFactory<Integer, ByteBuffer> pooledObjectFactory = new BaseKeyedPooledObjectFactory<>() {
            @Override
            public ByteBuffer create(Integer size) throws Exception {

                ByteBuffer allocate = ByteBuffer.allocate(size);

                if (Runtime.getRuntime().freeMemory() < 1024 * 1024 * 200) {
                    pool.setMaxTotalPerKey(200);
                } else {
                    pool.setMaxTotalPerKey(400);
                }
                System.out.println("Generating new ByteBuffer for pool. Free memory (MB): " +
                        (Runtime.getRuntime().freeMemory() / 1024. / 1024.) + " Idle: " + pool.getNumIdle() + " Active: " + pool.getNumActive() + " Waiters: " + pool.getNumWaiters());

                Map<String, List<DefaultPooledObjectInfo>> stringListMap = pool.listAllObjects();

                String out = "";

                for (String s : stringListMap.keySet()) {
                    out += "key: " + s + " size: " + stringListMap.get(s).size() + "\n";
                }


                System.out.println("\n\nList of Pool: \n" + out + "\n\n");

                return allocate;
            }

            @Override
            public void activateObject(Integer key, PooledObject<ByteBuffer> p) throws Exception {
                super.activateObject(key, p);
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
                System.out.println("validateObject: " + b);
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

            String out = "";
            for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
                out += e.toString() + "\n";
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
            String out = "";
            for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
                out += e.toString() + "\n";
            }
            byteBufferToStacktrace.put(byteBuffer, out);
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
