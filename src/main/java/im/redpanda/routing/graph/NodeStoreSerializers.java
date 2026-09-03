package im.redpanda.routing.graph;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import im.redpanda.identity.KademliaId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

/**
 * Explicit MapDB serializers for the node cache (T117).
 *
 * <p>Without them MapDB falls back to its default (Elsa), which writes fully qualified class names
 * and needs {@code Serializable} — the very coupling that made moving {@code core/Node.java} a
 * state-destroying change (DDD review §5). The key is the raw 20-byte KademliaId, the value the
 * same explicit JSON the node graph uses.
 *
 * <p>The serializers are set on all three cache tiers, not only the file-backed one: the on-heap
 * store keeps object references and never calls them, but the off-heap tier does serialize, and a
 * mixed configuration would keep {@code Serializable} alive for no reason.
 */
final class NodeStoreSerializers {

  static final Serializer<KademliaId> KADEMLIA_ID =
      new Serializer<>() {
        @Override
        public void serialize(DataOutput2 out, KademliaId value) throws IOException {
          Serializer.BYTE_ARRAY.serialize(out, value.getBytes());
        }

        @Override
        public KademliaId deserialize(DataInput2 in, int available) throws IOException {
          try {
            return new KademliaId(Serializer.BYTE_ARRAY.deserialize(in, available));
          } catch (IllegalArgumentException e) {
            throw new IOException("cached KademliaId has a wrong length", e);
          }
        }
      };

  static final Serializer<Node> NODE =
      new Serializer<>() {
        @Override
        public void serialize(DataOutput2 out, Node value) throws IOException {
          byte[] json =
              new Gson().toJson(NodeCodec.nodeToJson(value)).getBytes(StandardCharsets.UTF_8);
          Serializer.BYTE_ARRAY.serialize(out, json);
        }

        @Override
        public Node deserialize(DataInput2 in, int available) throws IOException {
          byte[] json = Serializer.BYTE_ARRAY.deserialize(in, available);
          final JsonObject object;
          try {
            object =
                JsonParser.parseString(new String(json, StandardCharsets.UTF_8)).getAsJsonObject();
          } catch (RuntimeException e) {
            throw new IOException("cached node is not a JSON object", e);
          }
          return NodeCodec.nodeFromJson(object);
        }
      };

  private NodeStoreSerializers() {}
}
