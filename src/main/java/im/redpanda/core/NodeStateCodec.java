package im.redpanda.core;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Explicit JSON mapping of the persisted identity/node types (T117): {@link NodeId} and {@link
 * Node} including its connection points.
 *
 * <p>These fragments carry no {@code format}/{@code version} header of their own — they are always
 * embedded in a document (or a MapDB map) whose name states the version, exactly like the {@code
 * mailboxItemsV2}/{@code seqCountersV1} convention introduced in T109.
 *
 * <p>The mapping is written out field by field on purpose. Reflection-based mapping would couple
 * the file to the Java field names and re-create the very problem T117 removes.
 */
public final class NodeStateCodec {

  /** Base64 of the 128-byte private export; present only for the node's own identity. */
  private static final String MEMBER_PRIVATE_KEY = "privateKey";

  /** Base64 of the 64-byte public export. */
  private static final String MEMBER_PUBLIC_KEY = "publicKey";

  private static final String MEMBER_LAST_SEEN = "lastSeen";

  private NodeStateCodec() {}

  /**
   * Encodes a NodeId with its private keys if it has them, otherwise with the public export only —
   * the same distinction the removed {@code writeObject} made with its leading boolean.
   *
   * @throws IllegalArgumentException if the NodeId has no keys at all (a bootstrap peer whose
   *     handshake never completed); callers must filter those out, they cannot be restored
   */
  public static JsonObject nodeIdToJson(NodeId nodeId) {
    if (!nodeId.hasKey()) {
      throw new IllegalArgumentException("cannot persist a NodeId without keys");
    }
    JsonObject json = new JsonObject();
    if (nodeId.hasPrivate()) {
      json.addProperty(MEMBER_PRIVATE_KEY, StateFormat.base64(nodeId.exportWithPrivate()));
    } else {
      json.addProperty(MEMBER_PUBLIC_KEY, StateFormat.base64(nodeId.exportPublic()));
    }
    return json;
  }

  /**
   * The export lengths are checked here, right against the constants, rather than through a generic
   * helper: {@link NodeId#importWithPrivate} and {@link NodeId#importPublic} reject a wrong length
   * with an {@code IllegalArgumentException}, and reading a state file must report a bad file as an
   * {@link IOException} (the caller regenerates), never as an unchecked exception.
   */
  public static NodeId nodeIdFromJson(JsonObject json) throws IOException {
    byte[] privateExport = StateFormat.optBase64(json, MEMBER_PRIVATE_KEY);
    if (privateExport != null) {
      if (privateExport.length != NodeId.PRIVATE_KEYLEN) {
        throw new IOException(
            "private NodeId export must be "
                + NodeId.PRIVATE_KEYLEN
                + " bytes but was "
                + privateExport.length);
      }
      try {
        return NodeId.importWithPrivate(privateExport);
      } catch (IllegalArgumentException e) {
        // right length, but the public halves do not match the private ones
        throw new IOException("could not read NodeId", e);
      }
    }

    byte[] publicExport = StateFormat.optBase64(json, MEMBER_PUBLIC_KEY);
    if (publicExport == null) {
      throw new IOException("NodeId holds neither a private nor a public key");
    }
    if (publicExport.length != NodeId.PUBLIC_KEYLEN) {
      throw new IOException(
          "public NodeId export must be "
              + NodeId.PUBLIC_KEYLEN
              + " bytes but was "
              + publicExport.length);
    }
    return NodeId.importPublic(publicExport);
  }

  /** Encodes a node of the DHT/graph state: its identity plus the counters that are persisted. */
  public static JsonObject nodeToJson(Node node) {
    JsonObject json = new JsonObject();
    json.add("nodeId", nodeIdToJson(node.getNodeId()));
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
    NodeId nodeId = nodeIdFromJson(StateFormat.requireObject(json, "nodeId"));

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
