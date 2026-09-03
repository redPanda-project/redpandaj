package im.redpanda.core;

import com.google.gson.JsonObject;
import java.io.IOException;

/**
 * Explicit JSON mapping of a persisted {@link NodeId} (T117).
 *
 * <p>These fragments carry no {@code format}/{@code version} header of their own — they are always
 * embedded in a document (or a MapDB map) whose name states the version, exactly like the {@code
 * mailboxItemsV2}/{@code seqCountersV1} convention introduced in T109.
 *
 * <p>The mapping is written out field by field on purpose. Reflection-based mapping would couple
 * the file to the Java field names and re-create the very problem T117 removes.
 *
 * <p>T118 split this out of {@code NodeStateCodec}: that class also encoded the routing graph's
 * {@code Node} through its package-private constructor, so it had to follow {@code Node} into
 * {@code im.redpanda.routing.graph} — while a {@code NodeId} is persisted by callers in three
 * different contexts (settings, peer list, node cache).
 */
public final class NodeIdCodec {

  /** Base64 of the 128-byte private export; present only for the node's own identity. */
  private static final String MEMBER_PRIVATE_KEY = "privateKey";

  /** Base64 of the 64-byte public export. */
  private static final String MEMBER_PUBLIC_KEY = "publicKey";

  private NodeIdCodec() {}

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
}
