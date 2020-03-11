package im.redpanda.store;

import im.redpanda.core.KademliaId;
import im.redpanda.core.Node;
import im.redpanda.core.Server;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NodeStore {

    public static ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(2);

    /**
     * These sizes are upper limits of the different dbs, the main eviction should be done via a timeout
     * after a get operation since the eviction by size is random.
     */
    private static final long MAX_SIZE_ONHEAP = 50L * 1024L * 1024L;
    private static final long MAX_SIZE_OFFHEAP = 50L * 1024L * 1024L;
    private static final long MAX_SIZE_ONDISK = 300L * 1024L * 1024L;

    private final HTreeMap onHeap;
    private final HTreeMap offHeap;
    private final HTreeMap onDisk;
    private final DB dbonHeap;
    private final DB dboffHeap;
    private final DB dbDisk;

    public NodeStore() {
        dbonHeap = DBMaker
                .heapDB()
                .closeOnJvmShutdown()
                .make();

        dboffHeap = DBMaker
                .memoryDirectDB()
                .closeOnJvmShutdown()
                .make();

        dbDisk = DBMaker
                .fileDB("data/nodeids" + Server.MY_PORT + ".mapdb")
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .checksumHeaderBypass()
                .make();

        onDisk = dbDisk
                .hashMap("nodeidsOnDisk")
                .expireStoreSize(MAX_SIZE_ONDISK)
                .expireExecutor(threadPool)
//                .expireAfterUpdate(60, TimeUnit.SECONDS) // no update since 14 days, not seen in this time
                .expireAfterGet(60, TimeUnit.DAYS)
                .createOrOpen();

        offHeap = dboffHeap
                .hashMap("nodeidsOffHeap")
                .expireStoreSize(MAX_SIZE_OFFHEAP)
                .expireOverflow(onDisk)
                .expireExecutor(threadPool)
                .expireAfterCreate()
                .expireAfterGet(60, TimeUnit.MINUTES)
                .create();

        onHeap = dbonHeap
                .hashMap("nodeidsOnHeap")
                .expireStoreSize(MAX_SIZE_ONHEAP)
                .expireOverflow(offHeap)
                .expireExecutor(threadPool)
                .expireAfterCreate()
                .expireAfterGet(15, TimeUnit.MINUTES)
                .create();
    }

    public void put(KademliaId kademliaId, Node node) {
        onHeap.put(kademliaId, node);
    }

    public Node get(KademliaId kademliaId) {
        try {
            return (Node) onHeap.get(kademliaId);
        } catch (Exception e) {
            e.printStackTrace();
            onDisk.clear();
            return null;
        }
    }

    public void saveToDisk() {

        offHeap.clearWithExpire();
        onHeap.clearWithExpire();
        offHeap.clearWithExpire();

//        System.out.println("save to disk: " + onHeap.size() + " " + offHeap.size() + " " + onDisk.size());

    }

    public void close() {

        saveToDisk();

        onHeap.close();
        offHeap.close();
        onDisk.close();

        dbonHeap.close();
        dboffHeap.close();
        dbDisk.close();


    }

    /**
     * Writes all to disk and then reads the size from the disk db.
     *
     * @return
     */
    public int size() {
        saveToDisk();
        return onDisk.size();
    }

//    put


}
