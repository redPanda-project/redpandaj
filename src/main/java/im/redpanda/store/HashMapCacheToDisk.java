package im.redpanda.store;

import org.mapdb.DB;
import org.mapdb.DBException;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class HashMapCacheToDisk extends HashMap<Long, Object> {

    public static final long MEAN_STORAGE_TIME = 1L * 60L * 1000L;
    public static final int NUMBER_OF_EVICTION_THREADS = 2;
    public static long TTL_ON_DISK = 24L * 60L * 60L * 1000L;
    public static long TTL_IN_MEMORY_OFF_HEAP = 10L * 60L * 1000L;

    LinkedBlockingQueue<Long> evictionQueue = new LinkedBlockingQueue<Long>();
    private final HTreeMap onDisk;
    private final HTreeMap inMemory;


    public HashMapCacheToDisk() {

        for (int i = 0; i < NUMBER_OF_EVICTION_THREADS; i++) {
            new EvictionThread().start();
        }


        DB dbDisk = DBMaker
                .fileDB("mapdbfile")
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .checksumHeaderBypass()
                .make();


        DB dbMemory = DBMaker
                .memoryDB()
                .closeOnJvmShutdown()
                .make();

// Big map populated with data expired from cache

        onDisk = dbDisk
                .hashMap("onDisk")
                .expireAfterCreate(TTL_ON_DISK) // time to keep on disk
                .expireExecutor(Executors.newScheduledThreadPool(2))
                .createOrOpen();


        inMemory = DBMaker
                .memoryShardedHashMap(16)
                .expireExecutor(
                        Executors.newScheduledThreadPool(3)
                )
                .expireAfterCreate(TTL_IN_MEMORY_OFF_HEAP) // keep 10 mins in off-heap serialized
                .expireOverflow(onDisk)
                .create();


//        inMemory.put("test", 2);
//
//        System.out.println("" + inMemory.get("test"));
//
//        int i = 0;
//        while (onDisk.get("test") == null) {
//            i++;
//            System.out.println("not found... " + i + " " + inMemory.getSize() + " " + onDisk.size());
//            byte[] bytes = new byte[1 * 1024 * 1024];
//            new Random().nextBytes(bytes);
//            inMemory.put(i, bytes);
//            Thread.sleep(10000);
//            System.out.println(" " + inMemory.getExpireMaxSize());
//        }

//        inMemory.close();
//        onDisk.clear();
//        onDisk.close();


    }


    public int sizeInMemory() {
        return inMemory.size();
    }


    public int sizeOnDisk() {
        return onDisk.size();
    }

    @Override
    public Object get(Object key) {

        Object o = super.get(key);

        // if the object is not stored in the Hashmap of this class we have to search it from mem and disk
        // this is handled by MapDB with the inMemory get command!
        if (o == null) {
            o = inMemory.get(key);
        }

        return o;
    }

    @Override
    public Object remove(Object key) {


        Object remove = super.remove(key);

        // if the object was not found we have to remove the object also from
        if (remove == null) {
            remove = inMemory.remove(key);
        }


        return remove;
    }

    public Object getNative(Object key) {
        return super.get(key);
    }


    public Object removeNative(Object key) {
        return super.remove(key);
    }

    @Override
    public Object put(Long key, Object value) {

        try {
            evictionQueue.put(key);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return super.put(key, value);
    }

    private class EvictionThread extends Thread {

        @Override
        public void run() {

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            while (true) {

                int cnt = evictionQueue.size();

                System.out.println("sleeping " + NUMBER_OF_EVICTION_THREADS * (long) Math.ceil(MEAN_STORAGE_TIME / cnt) + " ms, ojects stored in on-heap: " + cnt + " off-heap: " + inMemory.size() + " on disk: " + onDisk.size());

                if (cnt < 50) {
                    try {
                        Thread.sleep(10000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }


                try {
                    Thread.sleep(NUMBER_OF_EVICTION_THREADS * (long) Math.ceil(MEAN_STORAGE_TIME / cnt));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    Long take = evictionQueue.take();

//                        Object hasCreationTime = get(take);
//                        if (hasCreationTime == null) {
//                            System.out.println("object already removed! do nothing!");
//                            continue;
//                        }
//
//                        long waittime = System.currentTimeMillis() - hasCreationTime.getCreationTime();
//                        if (waittime > 0) {
//                            Thread.sleep(waittime);
//                        }
//
//                        // check if object still in hashmap
//                        hasCreationTime = get(take);
//                        if (hasCreationTime == null) {
//                            System.out.println("object already removed! do nothing!");
//                            continue;
//                        }

                    Object o = getNative(take);

                    if (o != null) {
                        // we now can remove that object and store it on mem/disk
                        removeNative(take);
                        inMemory.put(take, o);
                    }


                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    public void shutdown() {

    }
}
