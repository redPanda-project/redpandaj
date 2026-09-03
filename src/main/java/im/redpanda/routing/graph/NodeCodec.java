package im.redpanda.routing.graph;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import im.redpanda.core.NodeIdCodec;
import im.redpanda.core.StateFormat;
import im.redpanda.identity.NodeId;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Explicit JSON mapping of a persisted routing-graph {@link Node} including its connection points
 * (T117).
 *
 * <p>These fragments carry no {@code format}/{@code version} header of their own — they are always
 * embedded in a document (or a MapDB map) whose name states the version, exactly like the {@code
 * mailboxItemsV2}/{@code seqCountersV1} convention introduced in T109.
 *
 * <p>The mapping is written out field by field on purpose. Reflection-based mapping would couple
 * the file to the Java field names and re-create the very problem T117 removes. It also means this
 * codec must stay in {@code Node}'s package: it restores a node through the package-private
 * constructors of {@code Node} and {@code Node.ConnectionPoint} rather than widening them.
 */
public final class NodeCodec {

  private static final String MEMBER_LAST_SEEN = "lastSeen";

  private NodeCodec() {}

  /** Encodes a node of the DHT/graph state: its identity plus the counters that are persisted. */
  public static JsonObject nodeToJson(Node node) {
    JsonObject json = new JsonObject();
    json.add("nodeId", NodeIdCodec.nodeIdToJson(node.getNodeId()));
    json.addProperty(MEMBER_LAST_SEEN, node.getLastSeen());
    json.addProperty("gmTestsSuccessful", node.getGmTestsSuccessful());
    json.addProperty("gmTestsFailed", node.getGmTestsFailed());
    json.addProperty("blacklistedSince", node.blacklistedSince());

    JsonArray points = new JsonArray();
    for (Node.ConnectionPoint point : node.getConnectionPoints()) {
      JsonObject pointJson = new JsonObject();
      pointJson.addProperty("ip", point.getIp());
      pointJson.addProperty("port", point.getPort());
      pointJson.addProperty(MEMBER_LAST_SEEN, point.getLastSeen());
      pointJson.addProperty("retries", point.getRetries());
      points.add(pointJson);
    }
    json.add("connectionPoints", points);
    return json;
  }

  public static Node nodeFromJson(JsonObject json) throws IOException {
    NodeId nodeId = NodeIdCodec.nodeIdFromJson(StateFormat.requireObject(json, "nodeId"));

    ArrayList<Node.ConnectionPoint> points = new ArrayList<>();
    JsonElement pointsElement = json.get("connectionPoints");
    if (pointsElement != null && pointsElement.isJsonArray()) {
      for (JsonElement element : pointsElement.getAsJsonArray()) {
        if (!element.isJsonObject()) {
          throw new IOException("connectionPoints must hold objects");
        }
        JsonObject pointJson = element.getAsJsonObject();
        JsonElement ip = pointJson.get("ip");
        if (ip == null || !ip.isJsonPrimitive()) {
          throw new IOException("connection point without an ip");
        }
        points.add(
            new Node.ConnectionPoint(
                ip.getAsString(),
                StateFormat.optInt(pointJson, "port", 0),
                StateFormat.optLong(pointJson, MEMBER_LAST_SEEN, 0L),
                StateFormat.optInt(pointJson, "retries", 0)));
      }
    }

    return new Node(
        nodeId,
        StateFormat.optLong(json, MEMBER_LAST_SEEN, 0L),
        points,
        StateFormat.optInt(json, "gmTestsSuccessful", 0),
        StateFormat.optInt(json, "gmTestsFailed", 0),
        StateFormat.optLong(json, "blacklistedSince", 0L));
  }
}
