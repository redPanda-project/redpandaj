package im.redpanda.flaschenpost;

import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;

import java.nio.ByteBuffer;

public class GMParser {

    public static GMContent parse(ByteBuffer content) {


        byte type = (byte) content.get();


        if (type == GMType.GARLIC_MESSAGE.getId()) {

            GarlicMessage garlicMessage = new GarlicMessage(content);
            garlicMessage.tryParseContent();

            return garlicMessage;

        } else if (type == GMType.ACK.getId()) {

            GMAck gmAck = new GMAck(content);
            gmAck.parseContent();

//            System.out.println("obtained ack id: " + gmAck.getAckid());


            //            GMAck.
//
//            System.out.println("obtained GM ack for id: " + );
            return gmAck;
        }

        throw new RuntimeException("Unknown GMType at parsing: " + type);
    }

}
