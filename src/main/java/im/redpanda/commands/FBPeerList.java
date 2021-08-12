// automatically generated by the FlatBuffers compiler, do not modify

package im.redpanda.commands;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class FBPeerList extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static FBPeerList getRootAsFBPeerList(ByteBuffer _bb) { return getRootAsFBPeerList(_bb, new FBPeerList()); }
  public static FBPeerList getRootAsFBPeerList(ByteBuffer _bb, FBPeerList obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public FBPeerList __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public im.redpanda.commands.FBPeer peers(int j) { return peers(new im.redpanda.commands.FBPeer(), j); }
  public im.redpanda.commands.FBPeer peers(im.redpanda.commands.FBPeer obj, int j) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int peersLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public im.redpanda.commands.FBPeer.Vector peersVector() { return peersVector(new im.redpanda.commands.FBPeer.Vector()); }
  public im.redpanda.commands.FBPeer.Vector peersVector(im.redpanda.commands.FBPeer.Vector obj) { int o = __offset(4); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }

  public static int createFBPeerList(FlatBufferBuilder builder,
      int peersOffset) {
    builder.startTable(1);
    FBPeerList.addPeers(builder, peersOffset);
    return FBPeerList.endFBPeerList(builder);
  }

  public static void startFBPeerList(FlatBufferBuilder builder) { builder.startTable(1); }
  public static void addPeers(FlatBufferBuilder builder, int peersOffset) { builder.addOffset(0, peersOffset, 0); }
  public static int createPeersVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startPeersVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endFBPeerList(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }
  public static void finishFBPeerListBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedFBPeerListBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public FBPeerList get(int j) { return get(new FBPeerList(), j); }
    public FBPeerList get(FBPeerList obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

