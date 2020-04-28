package im.redpanda.jobs;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class JobScheduler extends ScheduledThreadPoolExecutor {

//    public static ScheduledThreadPoolExecutor threadpool = new (1);


    private static JobScheduler jobScheduler;

    static {
        jobScheduler = new JobScheduler(10, new SimpleNamingThreadFactory());
        jobScheduler.setKeepAliveTime(2, TimeUnit.MINUTES);
        jobScheduler.allowCoreThreadTimeOut(true);
    }

    public JobScheduler(int corePoolSize) {
        super(corePoolSize);
    }

    public JobScheduler(int corePoolSize, ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
    }

    public static ScheduledFuture insert(Runnable runnable, long delayInMS) {
        return jobScheduler.scheduleWithFixedDelay(runnable, delayInMS, delayInMS, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {

    }

    @Override
    protected void terminated() {

    }

    public static void runNow(Runnable command) {
        jobScheduler.execute(command);
    }

    static class SimpleNamingThreadFactory implements ThreadFactory {
        int number = 0;

        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("Jobs-" + number);
            number++;
            return thread;
        }
    }
}


