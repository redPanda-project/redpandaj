package im.redpanda.misc;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertTrue;

public class PoolTest {

    @Test
    public void test() throws Exception {
        BaseKeyedPooledObjectFactory<Integer, ByteBuffer> pooledObjectFactory = new BaseKeyedPooledObjectFactory<Integer, ByteBuffer>() {
            @Override
            public ByteBuffer create(Integer size) throws Exception {
//                System.out.println("create object" + size);
                switch (size) {
                    case 0:
                        return ByteBuffer.allocate(100);
                    case 1:
                        return ByteBuffer.allocate(200);
                    case 2:
                        return ByteBuffer.allocate(300);
                }
                return null;
            }

            @Override
            public void activateObject(Integer key, PooledObject<ByteBuffer> p) throws Exception {
//                System.out.println("act");
                super.activateObject(key, p);
            }

            @Override
            public void passivateObject(Integer key, PooledObject<ByteBuffer> p) throws Exception {
                p.getObject().position(0);
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


        GenericKeyedObjectPool<Integer, ByteBuffer> pool = new GenericKeyedObjectPool<>(pooledObjectFactory);


        for (int i = 0; i < 500; i++) {
            ByteBuffer byteBuffer = pool.borrowObject(1);

            assertTrue(byteBuffer.position() == 0);

            byteBuffer.putInt(1);
            pool.returnObject(1, byteBuffer);

//            System.out.println("Free memory (bytes): " +
//                    (Runtime.getRuntime().freeMemory() /1024./1024.));

        }

        assertTrue(pool.getNumIdle() == 1);


//        Thread.sleep(1000);

//        pool.setEvictionPolicy();

    }


}
