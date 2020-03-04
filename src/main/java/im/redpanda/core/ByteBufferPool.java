package im.redpanda.core;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.nio.Buffer;
import java.nio.ByteBuffer;

public class ByteBufferPool {


    private static GenericKeyedObjectPool<Integer, ByteBuffer> pool;

    public static void init() {
        BaseKeyedPooledObjectFactory<Integer, ByteBuffer> pooledObjectFactory = new BaseKeyedPooledObjectFactory<Integer, ByteBuffer>() {
            @Override
            public ByteBuffer create(Integer size) throws Exception {

                ByteBuffer allocate = ByteBuffer.allocate(size);

                if (Runtime.getRuntime().freeMemory() < 1024 * 1024 * 200) {
                    pool.setMaxTotalPerKey(2);
                } else {
                    pool.setMaxTotalPerKey(20);
                }
                System.out.println("Free memory (MB): " +
                        (Runtime.getRuntime().freeMemory() / 1024. / 1024.));

                return allocate;
            }

            @Override
            public void activateObject(Integer key, PooledObject<ByteBuffer> p) throws Exception {
//                System.out.println("act");
                super.activateObject(key, p);
            }

            @Override
            public void passivateObject(Integer key, PooledObject<ByteBuffer> p) throws Exception {
                ByteBuffer byteBuffer = p.getObject();

                byteBuffer.position(0);
                byteBuffer.limit(byteBuffer.capacity());


//                System.out.println("pass");
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
                return new DefaultPooledObject<ByteBuffer>(byteBuffer);
            }
        };


        pool = new GenericKeyedObjectPool<>(pooledObjectFactory);
        pool.setMinIdlePerKey(0);
    }

    public static GenericKeyedObjectPool<Integer, ByteBuffer> getPool() {
        return pool;
    }

    public static ByteBuffer borrowObject(Integer key) {

        key = keyToKey(key);

        System.out.println("borrow ByteBuffer key: " + key);

        ByteBuffer byteBuffer = null;
        try {
            byteBuffer = pool.borrowObject(key);
        } catch (Exception e) {
            e.printStackTrace();
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

        System.out.println("return ByteBuffer key: " + key);
        pool.returnObject(key, byteBuffer);
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
