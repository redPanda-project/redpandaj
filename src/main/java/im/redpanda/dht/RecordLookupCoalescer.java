package im.redpanda.dht;

import im.redpanda.identity.KademliaId;
import im.redpanda.mailbox.ReturnPath;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * T162/N5: per-node bookkeeping that merges client-triggered channel-record lookups ({@link
 * RecordLookupJob}) for the same key — but only while no network search has started for them.
 *
 * <p>A lookup waits out the randomized anti-profiling delay ({@link
 * RecordLookupJob#LOOKUP_DELAY_JITTER_MS}) before it searches. Lookups for the same key that arrive
 * in that window share the one search that follows; every caller still gets its own answer over its
 * own return path. Once the delay has run out the window is <em>closed</em>: a later caller starts
 * a fresh lookup instead of inheriting a search that began before it asked. That is deliberate — a
 * channel record is overwritten by every publication and pairing lives on reading the newest one,
 * so an answer must always come from a search started after the request.
 *
 * <p>There is intentionally <b>no result cache</b>: a positive cache could hand out a record the
 * counterpart has just replaced, a negative one would be exactly the false "no record" this class
 * exists to remove. Load stays bounded by the per-node lookup rate limit in {@code GarlicRouter}
 * (every admitted lookup starts at most one search).
 */
public final class RecordLookupCoalescer {

  /**
   * An open window older than this is treated as lost (its delayed search never ran; normally it
   * runs after at most {@link RecordLookupJob#LOOKUP_DELAY_JITTER_MS}). The next caller opens a new
   * window and inherits the waiting return paths.
   */
  static final long MAX_OPEN_MS = 10_000;

  /** The return paths waiting for one not-yet-started search. */
  static final class Window {
    private final List<ReturnPath> returnPaths = new ArrayList<>();
    private final long openedAtMs;
    private boolean taken = false;

    private Window(long openedAtMs) {
      this.openedAtMs = openedAtMs;
    }
  }

  private final Object lock = new Object();
  private final Map<KademliaId, Window> open = new HashMap<>();
  private long searchesScheduled = 0;

  /**
   * Attaches {@code returnPath} to the open window for {@code key}. Returns the window if the
   * caller opened it and must schedule the delayed search for it (and later {@link #close} it), or
   * {@code null} if the return path was attached to a window that is already scheduled.
   */
  Window join(KademliaId key, ReturnPath returnPath, long nowMs) {
    synchronized (lock) {
      Window current = open.get(key);
      if (current != null && nowMs - current.openedAtMs < MAX_OPEN_MS) {
        current.returnPaths.add(returnPath);
        return null;
      }
      Window window = new Window(nowMs);
      if (current != null) {
        // the old window's search never ran: take its callers over, and mark it so that a late
        // run of its job does not answer them a second time
        window.returnPaths.addAll(current.returnPaths);
        current.returnPaths.clear();
        current.taken = true;
      }
      window.returnPaths.add(returnPath);
      open.put(key, window);
      searchesScheduled++;
      return window;
    }
  }

  /**
   * Closes {@code window} when its search starts: no caller can attach any more. Returns the return
   * paths to answer — empty if the window was taken over by a newer one meanwhile.
   */
  List<ReturnPath> close(KademliaId key, Window window) {
    synchronized (lock) {
      open.remove(key, window);
      if (window.taken) {
        return List.of();
      }
      window.taken = true;
      return List.copyOf(window.returnPaths);
    }
  }

  /** Visible for testing: delayed searches scheduled so far. */
  long searchesScheduled() {
    synchronized (lock) {
      return searchesScheduled;
    }
  }

  /** Visible for testing: return paths waiting in the open window for {@code key}. */
  int waiting(KademliaId key) {
    synchronized (lock) {
      Window window = open.get(key);
      return window == null ? 0 : window.returnPaths.size();
    }
  }
}
