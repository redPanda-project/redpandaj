package im.redpanda.kademlia.nodeinfo;

import im.redpanda.core.NodeId;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GMEntryPointModel {

    private NodeId nodeId;
    private String ip;
    private int port;
    //todo add some connection points!!


    public GMEntryPointModel(NodeId nodeId) {
        this.nodeId = nodeId;
    }
}
