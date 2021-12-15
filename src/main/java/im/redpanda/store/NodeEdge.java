package im.redpanda.store;

import org.jgrapht.graph.DefaultEdge;

public class NodeEdge extends DefaultEdge {

    boolean lastCheckFailed = false;
    long timeLastCheckFailed = 0;
    long lastTimeCheckStarted = 0;

    public boolean isLastCheckFailed() {
        return lastCheckFailed;
    }

    public void setLastCheckFailed(boolean lastCheckFailed) {
        this.lastCheckFailed = lastCheckFailed;
        if (lastCheckFailed) {
            timeLastCheckFailed = System.currentTimeMillis();
        }
    }

    public long getTimeLastCheckFailed() {
        return timeLastCheckFailed;
    }


    public boolean isInLastTimeCheckWindow() {
        return System.currentTimeMillis() - lastTimeCheckStarted < 5000L;
    }

    public void touchLastTimeCheckStarted() {
        lastTimeCheckStarted = System.currentTimeMillis();
    }
}
