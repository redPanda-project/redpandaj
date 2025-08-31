package im.redpanda.flaschenpost;

import im.redpanda.core.ServerContext;
import org.junit.Test;

public class GMParserAdditionalTest {

    @Test(expected = RuntimeException.class)
    public void parseUnknownTypeThrows() {
        byte[] content = new byte[]{(byte) 99, 0, 0, 0};
        GMParser.parse(ServerContext.buildDefaultServerContext(), content);
    }
}

