package im.redpanda.flaschenpost;

import java.nio.ByteBuffer;

public class GMAck extends GMContent {

    final int ackid;

    public GMAck(int ackid) {
        this.ackid = ackid;
    }

    @Override
    protected void computeContent() {
        ByteBuffer allocate = ByteBuffer.allocate(1 + 4);
        allocate.put(getGMType().getId());
        allocate.putInt(ackid);

        setContent(allocate.array());
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

}

