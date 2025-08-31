package im.redpanda.crypt;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class UtilsIsLocalAddressTest {

    @Test
    public void testLocalAndNonLocalAddresses() {
        assertThat(im.redpanda.crypt.Utils.isLocalAddress("127.0.0.1"), is(true));
        assertThat(im.redpanda.crypt.Utils.isLocalAddress("localhost"), is(true));
        assertThat(im.redpanda.crypt.Utils.isLocalAddress("192.168.1.10"), is(true));
        assertThat(im.redpanda.crypt.Utils.isLocalAddress("10.0.0.1"), is(false));
        assertThat(im.redpanda.crypt.Utils.isLocalAddress("example.com"), is(false));
    }
}

