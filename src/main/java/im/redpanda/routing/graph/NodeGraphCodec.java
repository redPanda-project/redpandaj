package im.redpanda.routing.graph;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import im.redpanda.core.StateFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;

/**
 * Explicit JSON mapping of the persisted node graph (T117).
 *
 * <p>The graph is stored as a vertex array plus an edge array that references the vertices by their
 * index. That is what keeps the shared-instance semantics of the old Java-serialized graph: a node
 * that sits on several edges is one object after loading, not one copy per edge.
 *
 * <p>Lives in {@code store} rather than next to {@link NodeCodec} because {@link NodeEdge}'s state
 * is package-private here and must be restored as it was — {@code setLastCheckFailed(true)} would
 * stamp the current time into {@code timeLastCheckFailed}.
 */
public final class NodeGraphCodec {

  private NodeGraphCodec() {}

  public static JsonObject toJson(DefaultDirectedWeightedGraph<Node, NodeEdge> graph) {
    List<Node> vertices = new ArrayList<>(graph.vertexSet());
    Map<Node, Integer> indexOf = new HashMap<>();

    JsonArray verticesJson = new JsonArray();
    for (Node node : vertices) {
      indexOf.put(node, verticesJson.size());
      verticesJson.add(NodeCodec.nodeToJson(node));
    }

    JsonArray edgesJson = new JsonArray();
    for (NodeEdge edge : graph.edgeSet()) {
      JsonObject edgeJson = new JsonObject();
      edgeJson.addProperty("from", indexOf.get(graph.getEdgeSource(edge)));
      edgeJson.addProperty("to", indexOf.get(graph.getEdgeTarget(edge)));
      edgeJson.addProperty("weight", graph.getEdgeWeight(edge));
      edgeJson.addProperty("lastCheckFailed", edge.lastCheckFailed);
      edgeJson.addProperty("timeLastCheckFailed", edge.timeLastCheckFailed);
      edgeJson.addProperty("lastTimeCheckStarted", edge.lastTimeCheckStarted);
      edgesJson.add(edgeJson);
    }

    JsonObject json = new JsonObject();
    json.add("vertices", verticesJson);
    json.add("edges", edgesJson);
    return json;
  }

  public static DefaultDirectedWeightedGraph<Node, NodeEdge> fromJson(JsonObject json)
      throws IOException {
    DefaultDirectedWeightedGraph<Node, NodeEdge> graph =
        new DefaultDirectedWeightedGraph<>(NodeEdge.class);

    List<Node> vertices = new ArrayList<>();
    for (JsonElement element : requireArray(json, "vertices")) {
      if (!element.isJsonObject()) {
        throw new IOException("vertices must hold objects");
      }
      Node node = NodeCodec.nodeFromJson(element.getAsJsonObject());
      vertices.add(node);
      graph.addVertex(node);
    }

    for (JsonElement element : requireArray(json, "edges")) {
      if (!element.isJsonObject()) {
        throw new IOException("edges must hold objects");
      }
      JsonObject edgeJson = element.getAsJsonObject();
      Node source = vertexAt(vertices, StateFormat.optInt(edgeJson, "from", -1));
      Node target = vertexAt(vertices, StateFormat.optInt(edgeJson, "to", -1));
      if (source.equals(target)) {
        // the graph rejects loops; an edge like this cannot have been written by toJson, so a file
        // holding one is corrupt rather than merely old
        throw new IOException("edge from a vertex to itself");
      }
      NodeEdge edge = graph.addEdge(source, target);
      if (edge == null) {
        // duplicate edge - the graph keeps the first one, which carries the same endpoints
        continue;
      }
      graph.setEdgeWeight(edge, StateFormat.optDouble(edgeJson, "weight", 1d));
      edge.lastCheckFailed = StateFormat.optBoolean(edgeJson, "lastCheckFailed", false);
      edge.timeLastCheckFailed = StateFormat.optLong(edgeJson, "timeLastCheckFailed", 0L);
      edge.lastTimeCheckStarted = StateFormat.optLong(edgeJson, "lastTimeCheckStarted", 0L);
    }

    return graph;
  }

  private static Node vertexAt(List<Node> vertices, int index) throws IOException {
    if (index < 0 || index >= vertices.size()) {
      throw new IOException("edge references vertex " + index + " of " + vertices.size());
    }
    return vertices.get(index);
  }

  private static JsonArray requireArray(JsonObject json, String member) throws IOException {
    JsonElement element = json.get(member);
    if (element == null || !element.isJsonArray()) {
      throw new IOException("missing array member '" + member + "'");
    }
    return element.getAsJsonArray();
  }
}
