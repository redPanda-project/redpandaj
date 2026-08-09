package im.redpanda.kademlia.nodeinfo;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import im.redpanda.core.Log;
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
    } catch (AddressFormatException | IllegalArgumentException e) {
      /*
       * REDPANDAJ-2EH (second occurrence, TD032): this Base58 string is unauthenticated remote
       * input carried inside a self-signed nodeinfo KadContent. A valid Base58 string can still
       * decode to 64 bytes that BouncyCastle rejects as a non-canonical / small-order Ed25519
       * point with IllegalArgumentException("invalid public key") — the same rejection #299 fixed
       * at the handshake site. Left unhandled it escaped Gson out of NodeInfoModel.importFromString
       * and unwound the whole relay path (Sentry event per relay attempt, remaining commands in
       * the read dropped), and because the poisoned nodeinfo record is persisted in KadStoreManager
       * it was re-thrown on every later relay to that destination.
       *
       * Drop only this one entry point (return null) and keep its siblings instead of aborting the
       * entire nodeinfo parse. NodeId.importPublic keeps its throwing contract for local callers;
       * only this network-facing adapter is lenient. Consumers must skip a null nodeId — see
       * GMParser.sendGarlicMessageToPeer.
       */
      Log.put("dropping nodeinfo entry with unparseable node id: " + e.getMessage(), 50);
      return null;
    }
  }
}
