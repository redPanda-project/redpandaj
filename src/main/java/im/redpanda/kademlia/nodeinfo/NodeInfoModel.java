package im.redpanda.kademlia.nodeinfo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import im.redpanda.core.NodeId;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
public class NodeInfoModel {

  private static final Gson gson;

  static {
    gson = new GsonBuilder().registerTypeAdapter(NodeId.class, new NodeIdTypeAdapter()).create();
  }

  private final long timestamp = System.currentTimeMillis();
  @Setter @Getter private int uptime;
  @Getter private final List<GMEntryPointModel> entryPoints = new ArrayList<>();

  public static NodeInfoModel importFromString(String jsonString) {
    return gson.fromJson(jsonString, NodeInfoModel.class);
  }

  public void addEntryPoint(GMEntryPointModel entryPointModel) {
    entryPoints.add(entryPointModel);
  }

  public void addAllEntryPoints(List<GMEntryPointModel> entryPointModels) {
    entryPoints.addAll(entryPointModels);
  }

  public String export() {
    return gson.toJson(this);
  }
}
