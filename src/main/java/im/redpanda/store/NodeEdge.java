package im.redpanda.store;

import org.jgrapht.graph.DefaultEdge;

public class NodeEdge extends DefaultEdge {

    boolean lastCheckFailed = false;

    public boolean isLastCheckFailed() {
        return lastCheckFailed;
    }

    public void setLastCheckFailed(boolean lastCheckFailed) {
        this.lastCheckFailed = lastCheckFailed;
    }
}
