package im.redpanda.kademlia.nodeinfo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import im.redpanda.core.NodeId;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class NodeInfoModel {

    private static final Gson gson;

    static {
        gson = new GsonBuilder()
                .registerTypeAdapter(NodeId.class, new NodeIdTypeAdapter())
                .create();
    }

    private final long timestamp = System.currentTimeMillis();
    @Getter
    private final List<GMEntryPointModel> entryPoints = new ArrayList<>();

    public static NodeInfoModel importFromString(String jsonString) {
        return gson.fromJson(jsonString, NodeInfoModel.class);
    }

    public void addEntryPoint(GMEntryPointModel entryPointModel) {
        entryPoints.add(entryPointModel);
    }

    public String export() {
        return gson.toJson(this);
    }

}
