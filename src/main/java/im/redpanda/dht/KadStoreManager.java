package im.redpanda.dht;

import im.redpanda.core.ServerContext;
import im.redpanda.identity.KademliaId;
import im.redpanda.identity.NodeId;
import im.redpanda.identity.crypt.Base58;
import im.redpanda.identity.crypt.Sha256Hash;
import im.redpanda.identity.crypt.Utils;
import im.redpanda.ops.JobScheduler;
import im.redpanda.ops.Log;
import im.redpanda.routing.graph.Node;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.security.Security;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;

public class KadStoreManager {

  /**
   * Total stored content size (without keys) above which the eviction sweep additionally applies
   * the shorter, distance-based {@code keepTime} (space pressure). Below this threshold entries are
   * kept for the full {@link #MAX_KEEP_TIME}: for a random id the XOR distance to our node is
   * almost always ~160, which would collapse the distance-based retention to its 61-minute floor
   * and effectively wipe small stores (the pre-L6 behavior, see PR #279). A trailing {@code * 0}
   * used to zero this threshold out by accident.
   *
   * <p>T67 retention decision: expiry at {@code MAX_KEEP_TIME} is enforced on every node regardless
   * of store size — by the periodic sweep in {@link #put(KadContent)} and lazily in {@link
   * #get(KademliaId)} — so a node never serves or re-spreads a record that {@code put()} would
   * reject as too old. Only the distance-based shortening stays gated on this size.
   */
  private static final int MIN_SIZE = 1024 * 1024 * 10;

  /**
   * Hard retention limit: 14 days. {@code put()} rejects anything older, the eviction sweep drops
   * entries past this age even below {@link #MIN_SIZE}, and {@code get()} never returns them.
   */
  private static final long MAX_KEEP_TIME = 1000L * 60L * 60L * 24L * 14L;

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private static final Map<KademliaId, KadContent> entries = new HashMap<>();
  private static final ReentrantLock lock = new ReentrantLock();
  private static long lastCleanup = 0;
  private static int size = 0;
  private final ServerContext serverContext;

  public KadStoreManager(ServerContext serverContext) {
    this.serverContext = serverContext;
  }

  /**
   * basic put operation into our DHT Storage, if entry exists with same KadId, only the one with
   * the highest timestamp is kept. If timestamp is too far in the future, the content is ignored!
   *
   * @param content
   */
  public boolean put(KadContent content) {

    KademliaId id = content.getId();

    long currTime = System.currentTimeMillis();

    if (content.getTimestamp() - currTime > 1000L * 60L * 15L) {
      Log.put("Content for DHT entry is too new!", 50);
      return false;
    } else if (content.getTimestamp() < currTime - MAX_KEEP_TIME) {
      Log.put("Content for DHT entry is too old!", 50);
      return false;
    }

    boolean saved = false;

    lock.lock();
    try {
      KadContent foundContent = entries.get(id);

      if (foundContent == null || content.getTimestamp() > foundContent.getTimestamp()) {
        entries.put(id, content);
        size += content.getContent().length;
        if (foundContent != null) {
          size -= foundContent.getContent().length;
        }
        // System.out.println("stored");
        saved = true;
      }

      // todo max size!
      // The sweep runs regardless of store size (throttled to ~10 s) so entries older than
      // MAX_KEEP_TIME are evicted on every node, matching the put() age gate above (T67). The
      // shorter distance-based keepTime only kicks in under space pressure (> MIN_SIZE),
      // measured after the hard expiry pass so stale bulk cannot trigger it.
      if (currTime > lastCleanup + 1000L * 10L * 1L) {
        lastCleanup = currTime;

        // pass 1: hard expiry at MAX_KEEP_TIME, independent of store size
        expireEntriesOlderThan(currTime - MAX_KEEP_TIME);

        // pass 2: distance-based shortening, only if the post-expiry store is still above
        // MIN_SIZE
        if (size > MIN_SIZE) {
          ArrayList<KademliaId> kademliaIds = new ArrayList<>();

          for (KadContent c : entries.values()) {

            int distance = serverContext.getOwnNodeId().getDistance(c.getId());

            // long keepTime = (long) Math.ceil(MAX_KEEP_TIME * (160 - distance) / 160);
            long keepTime = (long) Math.ceil(1000L * 60L * 60L * 24L * (long) (160 - distance));

            keepTime =
                Math.max(keepTime, 1000L * 60L * 61L); // at least 61 mins such that the maintenance
            // routine can spread the entry
            keepTime = Math.min(keepTime, MAX_KEEP_TIME); // max time

            // System.out.println("keep time: " +
            // formatDuration(Duration.ofMillis(keepTime)) + " distance: " + distance);

            if (c.getTimestamp() < currTime - keepTime) {
              kademliaIds.add(c.getId());
              // entries.remove(c.getId());
              size -= c.getContent().length;
            }
          }

          for (KademliaId kadId : kademliaIds) {
            entries.remove(kadId);
          }
        }
      }

    } finally {
      lock.unlock();
    }

    return saved;
  }

  public KadContent get(Node node) {
    return get(KadContent.createKademliaId(node.getNodeId()));
  }

  /**
   * Returns the stored content for the given id, or {@code null} if there is none or it is older
   * than {@link #MAX_KEEP_TIME}. Entries past that age are expired lazily here because the eviction
   * sweep only runs from {@link #put(KadContent)} — a node that receives no puts must still never
   * serve a record that {@code put()} would reject as too old (T67).
   */
  public KadContent get(KademliaId id) {
    lock.lock();
    try {
      KadContent content = entries.get(id);
      if (content == null) {
        return null;
      }
      if (content.getTimestamp() < System.currentTimeMillis() - MAX_KEEP_TIME) {
        entries.remove(id);
        size -= content.getContent().length;
        return null;
      }
      return content;
    } finally {
      lock.unlock();
    }
  }

  public static void main(String[] args) {

    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

    // lets create a keypair for a DHT destination key, should be included in
    // channel later

    NodeId nodeId = new NodeId();

    // lets calculate the destination
    byte[] pubKey = nodeId.exportPublic();

    System.out.println("pubkey len: " + pubKey.length);

    Date date = new Date();
    SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    System.out.println("UTC Date is: " + dateFormat.format(date));

    byte[] dateStringBytes = dateFormat.format(date).getBytes();

    ByteBuffer buffer = ByteBuffer.allocate(pubKey.length + dateStringBytes.length);
    buffer.put(pubKey);
    buffer.put(dateStringBytes);

    Sha256Hash dhtKey = Sha256Hash.create(buffer.array());

    System.out.println(
        "" + Base58.encode(dhtKey.getBytes()) + " byteLen: " + dhtKey.getBytes().length);

    KademliaId kademliaId = KademliaId.fromFirstBytes(dhtKey.getBytes());

    // System.out.println("kadid: " + kademliaId.hexRepresentation());
    // System.out.println("kadid: " + Utils.bytesToHexString(dhtKey.getBytes()));

    System.out.println("kadid: " + kademliaId);

    // random content
    byte[] payload = new byte[1024];
    SECURE_RANDOM.nextBytes(payload);

    KadContent kadContent = new KadContent(nodeId.exportPublic(), payload);

    kadContent.signWith(nodeId);

    System.out.println(
        "signature: "
            + Utils.bytesToHexString(kadContent.getSignature())
            + " len: "
            + kadContent.getSignature().length);

    // lets check the signature

    System.out.println("verified: " + kadContent.verify());

    // assoziate an command pointer to the job
    HashMap<Integer, ScheduledFuture<?>> runningJobs = new HashMap<>();

    final int pointer = SECURE_RANDOM.nextInt();

    Job job = new Job(runningJobs, pointer);

    ScheduledFuture<?> future = JobScheduler.insert(job, 500);
    runningJobs.put(pointer, future);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    ScheduledFuture<?> scheduledFuture = runningJobs.get(pointer);

    Job r = job;

    boolean couldCancel = scheduledFuture.cancel(false);
    System.out.println("cancel: " + couldCancel);

    // if we are able to cancel the runnable, we have to transmit the new data to
    // the runnable
    if (couldCancel) {
      r.setData("new data");
      r.run();
    }

    System.out.println("asd");
  }

  public static void printStatus() {
    lock.lock();
    int size = 0;
    try {
      for (KademliaId id : entries.keySet()) {

        Duration duration =
            Duration.ofMillis(System.currentTimeMillis() - entries.get(id).getTimestamp());
        System.out.println(
            "id: "
                + id.toString()
                + " "
                + formatDuration(duration)
                + " "
                + Base58.encode(entries.get(id).createHash().getBytes()));
        size += entries.get(id).getContent().length;
      }
    } finally {
      lock.unlock();
    }
    System.out.println("size in kb: " + size / 1024.);
  }

  public static void maintain(ServerContext serverContext) {
    lock.lock();
    try {
      // expire instead of re-spreading entries every other node's put() would reject as too
      // old (T67) — maintain() may be the only caller on a node that receives no puts
      expireEntriesOlderThan(System.currentTimeMillis() - MAX_KEEP_TIME);
      for (KadContent kc : entries.values()) {
        new KademliaInsertJob(serverContext, kc).start();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Removes all entries with a timestamp older than the given cutoff and updates {@link #size}.
   * Callers must hold {@link #lock}.
   */
  private static void expireEntriesOlderThan(long cutoff) {
    Iterator<KadContent> iterator = entries.values().iterator();
    while (iterator.hasNext()) {
      KadContent c = iterator.next();
      if (c.getTimestamp() < cutoff) {
        size -= c.getContent().length;
        iterator.remove();
      }
    }
  }

  static class Job implements Runnable {

    HashMap<Integer, ScheduledFuture<?>> runningJobs;
    private final Integer pointer;
    private String data = null;

    public Job(HashMap<Integer, ScheduledFuture<?>> runningJobs, Integer pointer) {
      this.runningJobs = runningJobs;
      this.pointer = pointer;
    }

    boolean done = false;
    int timesRun = 0;

    @Override
    public void run() {

      System.out.println("asdf " + data + " done: " + done);

      if (done) {
        ScheduledFuture<?> sf = runningJobs.remove(pointer);
        sf.cancel(false);
      }
      timesRun++;
    }

    public void setData(String str) {
      data = str;
      done = true;
    }
  }

  public static String formatDuration(Duration duration) {
    long seconds = duration.getSeconds();
    long absSeconds = Math.abs(seconds);
    String positive =
        "%d:%02d:%02d".formatted(absSeconds / 3600, absSeconds % 3600 / 60, absSeconds % 60);
    return seconds < 0 ? "-" + positive : positive;
  }
}
