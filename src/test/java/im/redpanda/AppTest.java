package im.redpanda;

import io.sentry.Sentry;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class AppTest {

    App app = new App();

    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        assertTrue(true);
    }

    @Test
    public void testGetHelloWorld() {
        assertEquals("Hello World!", app.getHelloWorld());
    }

    @Test
    public void testSentry() {
        Sentry.init("https://eefa8afdcdb7418995f6306c136546c7@sentry.io/1400313");
    }

}
