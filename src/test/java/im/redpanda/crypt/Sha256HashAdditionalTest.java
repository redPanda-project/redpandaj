package im.redpanda.crypt;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.util.Arrays;
import org.junit.Test;

public class Sha256HashAdditionalTest {

  @Test(expected = IllegalArgumentException.class)
  public void constructor_rejectsWrongLength() {
    new Sha256Hash(new byte[31]);
  }

  @Test
  public void equalsHashCodeAndToString_work() {
    byte[] data = new byte[32];
    Arrays.fill(data, (byte) 1);

    Sha256Hash h1 = new Sha256Hash(data);
    Sha256Hash h2 = new Sha256Hash(h1.toString());

    assertEquals(h1, h2);
    assertEquals(h1.hashCode(), h2.hashCode());
    assertEquals(h1.toString(), h2.toString());
    assertArrayEquals(h1.getBytes(), h2.getBytes());

    Sha256Hash dup = h1.duplicate();
    assertEquals(h1, dup);
    assertEquals(new BigInteger(1, data), h1.toBigInteger());
  }
}
