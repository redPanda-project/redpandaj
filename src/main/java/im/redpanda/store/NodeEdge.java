package im.redpanda.store;

import im.redpanda.jobs.PeerPerformanceTestGarlicMessageJob;
import org.jgrapht.graph.DefaultEdge;

public class NodeEdge extends DefaultEdge implements Comparable<NodeEdge> {

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
    return System.currentTimeMillis() - lastTimeCheckStarted
        < PeerPerformanceTestGarlicMessageJob.JOB_TIMEOUT;
  }

  public void touchLastTimeCheckStarted() {
    lastTimeCheckStarted = System.currentTimeMillis();
  }

  @Override
  public int compareTo(NodeEdge o) {
    long longValue = lastTimeCheckStarted - o.lastTimeCheckStarted;
    if (longValue > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }

    if (longValue < Integer.MIN_VALUE) {
      return Integer.MIN_VALUE;
    }
    return (int) longValue;
  }
}
