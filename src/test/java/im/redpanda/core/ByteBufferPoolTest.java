package im.redpanda.core;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class ByteBufferPoolTest {

    static {
        ByteBufferPool.init();
    }

    @Test
    public void testLimits() {


        boolean limit = false;


        for (int i = 0; i < 15; i++) {
            ByteBuffer byteBuffer = ByteBufferPool.borrowObject(20 * 1024 * 1024);
            limit = ByteBufferPool.getPool().getMaxTotalPerKey() < 10;
            if (limit) {
                break;
            }
        }

        assertTrue(limit);
    }

}