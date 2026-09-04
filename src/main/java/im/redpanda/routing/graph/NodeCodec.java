package im.redpanda.routing.graph;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import im.redpanda.core.NodeIdCodec;
import im.redpanda.core.StateFormat;
import im.redpanda.identity.NodeId;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

  private static final Logger logger = LogManager.getLogger();

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
      if (point.getIp() == null) {
        // T150: Gson turns a null String into a JSON null, and nodeFromJson below used to throw
        // on reading one back -- writer and reader disagreed, so one unpersistable point made the
        // whole node (and, through MapDB's overflow, the whole node cache) unreadable. Both ends
        // are fixed: nothing without an ip is written here, and the reader drops rather than
        // rejects what an older file already holds. Node itself no longer accepts such a point
        // either; skipping it here keeps a node object that predates that guard -- restored from
        // an old file, or built by a test -- persistable instead of poisoning the store.
        continue;
      }
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
          // T150: this used to throw. A connection point is a dialable-address hint, not part of
          // the node's identity, so one unusable hint must not cost the node -- and, since MapDB
          // deserializes whole tiers inside clearWithExpire(), must not cost the entire node
          // cache and with it every Node.getByKademliaId() call of the running process. The
          // identity (nodeId) below stays strict: a node we cannot name is genuinely corrupt.
          logger.warn(
              "dropping a persisted connection point without an ip, node {}: {}",
              nodeId.getKademliaId(),
              pointJson);
          continue;
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
