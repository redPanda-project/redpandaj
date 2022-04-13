package im.redpanda.kademlia;

import im.redpanda.core.KademliaId;
import im.redpanda.core.Log;
import im.redpanda.core.Node;
import im.redpanda.core.NodeId;
import im.redpanda.core.ServerContext;
import im.redpanda.crypt.Base58;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.Utils;
import im.redpanda.jobs.JobScheduler;
import im.redpanda.jobs.KademliaInsertJob;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.security.Security;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class KadStoreManager {

    private static final int MIN_SIZE = 1024 * 1024 * 10 * 0; //size of content without key
    private static final int MAX_SIZE = 1024 * 1024 * 50; //size of content without key
    private static final long MAX_KEEP_TIME = 1000L * 60L * 60L * 24L * 14L; //7 days

    private static final Map<KademliaId, KadContent> entries = new HashMap<>();
    private static final ReentrantLock lock = new ReentrantLock();
    private static long lastCleanup = 0;
    private static int size = 0;
    private final ServerContext serverContext;


    public KadStoreManager(ServerContext serverContext) {
        this.serverContext = serverContext;
    }

    /**
     * basic put operation into our DHT Storage, if entry exists with same KadId,
     * only the one with the highest timestamp is kept.
     * If timestamp is too far in the future, the content is ignored!
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
//                System.out.println("stored");
                saved = true;
            }


            //todo max size!
            if (size > MIN_SIZE && currTime > lastCleanup + 1000L * 10L * 1L) {
                lastCleanup = currTime;

                ArrayList<KademliaId> kademliaIds = new ArrayList<>();

                for (KadContent c : entries.values()) {


                    int distance = serverContext.getNonce().getDistance(c.getId());


//                    long keepTime = (long) Math.ceil(MAX_KEEP_TIME * (160 - distance) / 160);
                    long keepTime = (long) Math.ceil(1000L * 60L * 60L * 24L * (long) (160 - distance));

                    keepTime = Math.max(keepTime, 1000L * 60L * 61L); //at least 61 mins such that the maintenance routine can spread the entry
                    keepTime = Math.min(keepTime, MAX_KEEP_TIME); // max time

//                    System.out.println("keep time: " + formatDuration(Duration.ofMillis(keepTime)) + " distance: " + distance);
//                    System.out.println("id: " + Server.NONCE);
//                    System.out.println("id: " + c.getId());

                    //todo: shorter times for key far away from our id
                    if (c.getTimestamp() < currTime - keepTime) {
                        kademliaIds.add(c.getId());
//                        entries.remove(c.getId());
                        size -= c.getContent().length;
                    }
                }

                for (KademliaId kadId : kademliaIds) {
                    entries.remove(kadId);
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

    public KadContent get(KademliaId id) {
        lock.lock();
        try {
            return entries.get(id);
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) {

        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

        //lets create a keypair for a DHT destination key, should be included in channel later

        NodeId nodeId = new NodeId();


        //lets calculate the destination
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

        System.out.println("" + Base58.encode((dhtKey.getBytes())) + " byteLen: " + dhtKey.getBytes().length);

        KademliaId kademliaId = KademliaId.fromFirstBytes(dhtKey.getBytes());

//        System.out.println("kadid: " + kademliaId.hexRepresentation());
//        System.out.println("kadid: " + Utils.bytesToHexString(dhtKey.getBytes()));

        System.out.println("kadid: " + kademliaId);

        //random content
        byte[] payload = new byte[1024];
        new Random().nextBytes(payload);


        KadContent kadContent = new KadContent(nodeId.exportPublic(), payload);

        kadContent.signWith(nodeId);

        System.out.println("signature: " + Utils.bytesToHexString(kadContent.getSignature()) + " len: " + kadContent.getSignature().length);


        //lets check the signature

        System.out.println("verified: " + kadContent.verify());


        //assoziate an command pointer to the job
        HashMap<Integer, ScheduledFuture> runningJobs = new HashMap<>();


        final int pointer = new Random().nextInt();

        Job job = new Job(runningJobs, pointer);


        ScheduledFuture future = JobScheduler.insert(job, 500);
        runningJobs.put(pointer, future);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ScheduledFuture scheduledFuture = runningJobs.get(pointer);

        Job r = job;

        boolean couldCancel = scheduledFuture.cancel(false);
        System.out.println("cancel: " + couldCancel);


        //if we are able to cancel the runnable, we have to transmit the new data to the runnable
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

                Duration duration = Duration.ofMillis(System.currentTimeMillis() - entries.get(id).getTimestamp());
                System.out.println("id: " + id.toString() + " " + formatDuration(duration) + " " + Base58.encode(entries.get(id).createHash().getBytes()));
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
            for (KadContent kc : entries.values()) {
                new KademliaInsertJob(serverContext, kc).start();
            }
        } finally {
            lock.unlock();
        }
    }


    static class Job implements Runnable {

        HashMap<Integer, ScheduledFuture> runningJobs;
        private final Integer pointer;
        private String data = null;

        public Job(HashMap<Integer, ScheduledFuture> runningJobs, Integer pointer) {
            this.runningJobs = runningJobs;
            this.pointer = pointer;
        }

        boolean done = false;
        int timesRun = 0;

        @Override
        public void run() {


            System.out.println("asdf " + data + " done: " + done);

            if (done) {
                ScheduledFuture sf = runningJobs.remove(pointer);
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
        String positive = String.format(
                "%d:%02d:%02d",
                absSeconds / 3600,
                (absSeconds % 3600) / 60,
                absSeconds % 60);
        return seconds < 0 ? "-" + positive : positive;
    }
}
