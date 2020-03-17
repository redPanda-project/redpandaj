package im.redpanda.flaschenpost;


import im.redpanda.core.KademliaId;
import im.redpanda.core.Peer;
import im.redpanda.core.PeerList;
import im.redpanda.core.Server;


public class Flaschenpost {


    public static void put(KademliaId destination, byte[] content) {


        //let us get the closest node in our peerlist to the destination

        // we only want to find peers which are closer to destination than we are
        int nearestDistance = destination.getDistance(Server.NONCE);
        Peer nearestPeer = null;

        System.out.println("our distance: " + nearestDistance);

        PeerList.getReadWriteLock().readLock().lock();
        try {
            for (Peer p : PeerList.getPeerArrayList()) {

                if (p.getNodeId() == null) {
                    continue;
                }

                int distance = p.getKademliaId().getDistance(destination);
                System.out.println("distance: " + distance);
                if (distance < nearestDistance) {
                    nearestDistance = distance;
                    nearestPeer = p;
                }

            }


            if (nearestPeer == null) {
                System.out.println("we are the closest peer!");
            } else {
                System.out.println("found peer with distance: " + nearestDistance + " peer: " + nearestPeer.getNodeId());
            }


        } finally {
            PeerList.getReadWriteLock().readLock().unlock();
        }


    }

}
