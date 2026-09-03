/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;

/**
 * The persisted form of a dialable {@link Peer}: address plus identity. Written by {@link Saver} as
 * explicit JSON since T117 — not {@code Serializable} any more, so a package move cannot make the
 * peer list unreadable.
 *
 * @author robin
 */
public class PeerSaveable {

  String ip;
  int port;
  NodeId nodeId;
  int retries;

  public PeerSaveable(String ip, int port, NodeId nodeId, int retries) {
    this.ip = ip;
    this.port = port;
    this.nodeId = nodeId;
    this.retries = retries;
  }

  public Peer toPeer() {
    Peer out = new Peer(ip, port);
    out.setNodeId(nodeId);
    out.retries = retries;
    return out;
  }
}
