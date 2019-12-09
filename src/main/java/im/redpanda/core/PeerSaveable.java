/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package im.redpanda.core;

import java.io.Serializable;

/**
 * @author robin
 */
public class PeerSaveable implements Serializable {

    String ip;
    int port;
    KademliaId nonce;
    int retries;


    public PeerSaveable(String ip, int port, KademliaId nonce, int retries) {
        this.ip = ip;
        this.port = port;
        this.nonce = nonce;
        this.retries = retries;
    }

    public Peer toPeer() {
        Peer out = new Peer(ip, port);
        out.setKademliaId(nonce);
        out.retries = retries;
        return out;
    }
}
