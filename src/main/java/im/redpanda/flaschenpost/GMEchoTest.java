package im.redpanda.flaschenpost;

import java.nio.ByteBuffer;

public class GMEchoTest extends GMContent {

    protected byte TYPE = 1;

    @Override
    protected void computeContent() {
        String text = "test";
        ByteBuffer allocate = ByteBuffer.allocate(1 + text.length());
        allocate.put(getGMType().getId());
        allocate.put(text.getBytes());
        content = allocate.array();
    }

    @Override
    public GMType getGMType() {
        return GMType.ECHO;
    }
}
