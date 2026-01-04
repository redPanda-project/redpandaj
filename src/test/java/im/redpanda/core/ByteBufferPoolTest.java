package im.redpanda.core;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import org.junit.Ignore;
import org.junit.Test;

public class ByteBufferPoolTest {

  static {
    ByteBufferPool.init();
  }

  @Test
  @Ignore("Does not work if too many RAM available?")
  public void testLimits() {

    boolean limit = false;

    for (int i = 0; i < 15; i++) {
      ByteBufferPool.borrowObject(20 * 1024 * 1024);
      limit = ByteBufferPool.getPool().getMaxTotalPerKey() < 10;
      if (limit) {
        break;
      }
    }

    assertTrue(limit);
  }

  @Test
  public void borrowTest() {
    ByteBuffer byteBufferFirst = ByteBufferPool.borrowObject(512);
    ByteBuffer byteBufferSecond = ByteBufferPool.borrowObject(512);

    byteBufferFirst.putInt(5);

    assertNotEquals(byteBufferFirst, byteBufferSecond);

    byteBufferFirst.flip();
    byteBufferFirst.getInt();
    byteBufferFirst.compact();

    ByteBufferPool.returnObject(byteBufferFirst);
    ByteBuffer byteBufferReturned = ByteBufferPool.borrowObject(512);

    assertEquals(byteBufferFirst, byteBufferReturned);
  }

  @Test
  public void defectReturnedObject() {

    ByteBuffer buffer = ByteBufferPool.borrowObject(512);
    buffer.putInt(1);
    ByteBufferPool.returnObject(buffer);

    buffer = ByteBufferPool.borrowObject(512);

    assertEquals(0, buffer.position());
  }
}
