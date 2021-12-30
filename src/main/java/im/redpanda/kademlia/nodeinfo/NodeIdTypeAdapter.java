package im.redpanda.kademlia.nodeinfo;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import im.redpanda.core.NodeId;
import im.redpanda.crypt.AddressFormatException;
import im.redpanda.crypt.Base58;

import java.io.IOException;

public class NodeIdTypeAdapter extends TypeAdapter<NodeId> {

    @Override
    public void write(JsonWriter jsonWriter, NodeId nodeId) throws IOException {
        jsonWriter.value(Base58.encode(nodeId.exportPublic()));
    }

    @Override
    public NodeId read(JsonReader jsonReader) throws IOException {
        String s = jsonReader.nextString();
        try {
            return NodeId.importPublic(Base58.decode(s));
        } catch (AddressFormatException e) {
            e.printStackTrace();
            throw new IOException("public node id string could not be parsed");
        }
    }
}
