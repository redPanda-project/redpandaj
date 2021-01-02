package im.redpanda.flaschenpost;

import im.redpanda.core.Server;

import java.nio.ByteBuffer;

public class GMAck extends GMContent {

    int ackid;

    public GMAck(int ackid) {
        this.ackid = ackid;
    }

    public GMAck() {
        this.ackid = Server.random.nextInt();
    }

    public GMAck(byte[] contentToParse) {

//        ByteBuffer buffer = ByteBuffer.wrap(contentToParse);
//
//        byte[] content = new byte[4];
//
//        buffer.get(content);

        setContent(contentToParse);
    }

    @Override
    protected void computeContent() {
        ByteBuffer allocate = ByteBuffer.allocate(1 + 4 + 4);
        allocate.put(getGMType().getId());
        allocate.putInt(4); // bytes after this int
        allocate.putInt(ackid);

        setContent(allocate.array());
    }

    protected void parseContent() {
        ByteBuffer buffer = ByteBuffer.wrap(getContent());

        byte type = buffer.get();

        if (type != getGMType().getId()) {
            throw new RuntimeException("ackid should have an ack id type!");
        }

        int remainingLen = buffer.getInt();

        if (remainingLen != 4) {
            throw new RuntimeException("len of GMAck wrong...." + remainingLen);
        }

        ackid = buffer.getInt();
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

