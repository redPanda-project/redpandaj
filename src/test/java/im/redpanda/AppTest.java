package im.redpanda;

import im.redpanda.store.HashMapCacheToDisk;
import io.sentry.Sentry;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class AppTest {

    App app = new App();

    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        assertTrue(true);
    }

//    @Test
//    public void testSentry() {
//        Sentry.init("https://eefa8afdcdb7418995f6306c136546c7@sentry.io/1400313");
//    }

//    @Test
//    public void testMapDB() throws InterruptedException {
//
//        new File("mapdbfile").delete();
//
//        DB dbDisk = DBMaker
//                .fileDB("mapdbfile")
//                .fileMmapEnableIfSupported()
//                .make();
//
//        DB dbMemory = DBMaker
//                .memoryDB()
//                .make();
//
//// Big map populated with data expired from cache
//        HTreeMap onDisk = dbDisk
//                .hashMap("onDisk")
//                .expireAfterCreate(2000L)
//                .expireExecutor(Executors.newScheduledThreadPool(2))
//                .createOrOpen();
//
//
//        HTreeMap inMemory = DBMaker
//                .memoryShardedHashMap(16)
//                .expireExecutor(
//                        Executors.newScheduledThreadPool(3)
//                )
//                .expireAfterCreate(2000L)
//                .expireOverflow(onDisk)
//                .create();
//
//
////        HTreeMap inMemory = dbMemory
////                .hashMap("inMemory")
////                .expireStoreSize(10 * 1024 * 1024)
////                .expireMaxSize(10)
////                .expireAfterCreate()
//////                .expireAfterGet()
////                //this registers overflow to `onDisk`
////                .expireOverflow(onDisk)
////                //good idea is to enable background expiration
////                .expireExecutor(Executors.newScheduledThreadPool(2))
////                .create();
//
//
//        inMemory.put("test", 2);
//
//        System.out.println("" + inMemory.get("test"));
//
//        int i = 0;
////        while (onDisk.get("test") == null) {
//        while (i >= 0) {
//            i++;
//            System.out.println("not found... " + i + " " + inMemory.getSize() + " " + onDisk.size());
//            byte[] bytes = new byte[1 * 1024];
//            new Random().nextBytes(bytes);
//            inMemory.put(i, bytes);
//            Thread.sleep(1000);
//            System.out.println(" " + inMemory.getExpireMaxSize());
//        }
//
//        inMemory.close();
//        onDisk.clear();
//        onDisk.close();
//
//    }


//    @Test
//    public void testHashMapCache() throws InterruptedException {
//
//        HashMapCacheToDisk.TTL_IN_MEMORY_OFF_HEAP = 50;
//        HashMapCacheToDisk.TTL_ON_DISK = 50;
//
//        HashMapCacheToDisk map = new HashMapCacheToDisk();
//
//
////        long i = 0;
////        while (i < 2000) {
////            i++;
////            map.put(i, "test");
////            System.out.println("" + i);
////        }
//
//        long i = 0;
//        while (i < 500) {
//            i++;
//            map.put(i, "test");
//            System.out.println("" + i);
//        }
//
//
//        assertTrue(map.size() == 500);
//
//        Thread.sleep(1000L);
//
//        assertTrue(map.size() < 500);
//        assertTrue(map.sizeInMemory() > 0);
//
//        Thread.sleep(1000L);
//
//        assertTrue(map.size() <500);
//        assertTrue(map.sizeInMemory() <500);
//        assertTrue(map.sizeOnDisk() > 0);
//
//
//    }

}
