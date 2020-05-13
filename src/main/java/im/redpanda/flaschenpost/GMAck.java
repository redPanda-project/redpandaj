package im.redpanda.flaschenpost;

import java.nio.ByteBuffer;

public class GMAck extends GMContent {

    int ackid;

    public GMAck(int ackid) {
        this.ackid = ackid;
    }

    public GMAck(ByteBuffer buffer) {

        byte[] content = new byte[4];

        buffer.get(content);

        setContent(content);
    }

    @Override
    protected void computeContent() {
        ByteBuffer allocate = ByteBuffer.allocate(1 + 4);
        allocate.put(getGMType().getId());
        allocate.putInt(ackid);

        setContent(allocate.array());
    }

    protected void parseContent() {
        ackid = ByteBuffer.wrap(getContent()).getInt();
    }

    @Override
    public GMType getGMType() {
        return GMType.ACK;
    }

//    @Override
//    public GMContent fromContent(byte[] bytes) {
//        ByteBuffer wrap = ByteBuffer.wrap(bytes);
//        int ackId = wrap.getInt();
//        return new GMAck(ackId);
//    }


    public int getAckid() {
        return ackid;
    }
}

