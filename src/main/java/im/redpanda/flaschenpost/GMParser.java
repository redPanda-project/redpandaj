package im.redpanda.flaschenpost;

import java.nio.ByteBuffer;

public class GMParser {

    public static void parse(byte[] bytes) {

        ByteBuffer wrap = ByteBuffer.wrap(bytes);

        byte type = (byte) wrap.get();


        if (type == GMType.GARLIC_MESSAGE.getId()) {


        } else if (type == GMType.ACK.getId()) {
            //            GMAck.
//
//            System.out.println("obtained GM ack for id: " + );
        }


    }

}
