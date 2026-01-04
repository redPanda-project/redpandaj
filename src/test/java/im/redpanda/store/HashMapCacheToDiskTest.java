package im.redpanda.store;

import static org.junit.Assert.*;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public class HashMapCacheToDiskTest {

  @Test
  public void test() {

    HashMapCacheToDisk<Long, String> longStringHashMapCacheToDisk = new HashMapCacheToDisk<>();

    longStringHashMapCacheToDisk.put(123L, "test");

    assertTrue(longStringHashMapCacheToDisk.get(123L).equals("test"));
  }

  @Test
  public void test2() {

    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);

    new File("data").mkdirs();

    DB dbDisk =
        DBMaker.fileDB("data/test2.mapdb")
            .fileMmapEnableIfSupported()
            .checksumHeaderBypass()
            .make();

    DB dbMemory =
        DBMaker
            // .memoryDB()
            .heapDB()
            .make();

    /**
     * if the max size of 300 MB is reached, the entries will be randomly delete from disk this is
     * only for a worst case scenario and the entries should be evicted by time
     */
    HTreeMap onDisk =
        dbDisk
            .hashMap("onDisk")
            // .expireStoreSize(300 * 1024 * 1024)
            .expireExecutor(scheduledExecutorService)
            // .expireAfterUpdate(14, TimeUnit.DAYS) // no update since 14 days, not seen in
            // this time
            .expireAfterUpdate(
                60, TimeUnit.SECONDS) // no update since 14 days, not seen in this time
            .createOrOpen();

    HTreeMap inMemory =
        dbMemory
            .hashMap("inMemory")
            // .expireStoreSize(5 * 1024 * 1024)
            // .expireMaxSize(5)
            .expireOverflow(onDisk)
            .expireExecutor(scheduledExecutorService)
            .expireAfterGet(1000L)
            // .expireAfterUpdate()
            .expireAfterCreate()
            .create();

    inMemory.put(-1, 2);

    System.out.println(inMemory.size() + " " + onDisk.size());

    inMemory.clearWithExpire();

    System.out.println("" + inMemory.get("test"));

    System.out.println("" + inMemory.isForegroundEviction());

    // int i = 0;
    // while (i < 100) {
    // i++;
    // System.out.println(i + " - " + inMemory.size() + " " + onDisk.size());
    //// if (i < 50) {
    //// Object o = inMemory.get(-1);
    //// }
    // try {
    // Thread.sleep(300);
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    // }

    // int i = 0;
    // while (onDisk.get("test22222222") == null) {
    //
    // i++;
    // System.out.println("not found... " + i + " " + inMemory.getSize() + " " +
    // onDisk.size());
    // TestClass testClass = new TestClass(i);
    //// new Random().nextBytes(bytes);
    // inMemory.put(i, testClass);
    // try {
    // Thread.sleep(300);
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // }
    //
    //
    // TestClass testClass1 = (TestClass) inMemory.get(i);
    // testClass1.setI(testClass1.getI() + 1);
    // inMemory.put(i, testClass1);
    //
    //// System.out.println("asd: " + testClass1.getI());
    //
    // testClass1 = (TestClass) inMemory.get(i);
    // System.out.println(" i : " + i + " newval: " + testClass1.getI());
    //
    //// Set set = onDisk.keySet();
    ////
    //// ArrayList<Integer> ints = new ArrayList<>();
    //// ints.addAll(set);
    //// Collections.sort(ints);
    ////
    ////
    //// for (Integer anInt : ints) {
    //// System.out.println("item: " + anInt);
    //// }
    //
    //// onDisk.forEachKey(o -> {
    //// System.out.println("item: " + o.toString());
    //// return o;
    //// });
    //
    // }

    onDisk.verify();
    // onDisk.clear();

    inMemory.close();
    onDisk.close();
  }
}
