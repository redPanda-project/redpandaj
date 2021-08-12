package im.redpanda.jobs;

import im.redpanda.core.ServerContext;

public class TestJob extends Job {


    private String someData = "test";

    public TestJob(ServerContext serverContext) {
        super(serverContext);
    }

    public void setSomeData(String someData) {
        this.someData = someData;
    }

    public String getSomeData() {
        return someData;
    }

    @Override
    public void init() {

    }

    @Override
    public void work() {
        System.out.println("run: " + someData);


        if (someData.equals("new data")) {
            done();
        }

        if (getEstimatedRuntime() >= 10000) {
            done();
        }

    }


    public static void main(String[] args) throws InterruptedException {
        ServerContext serverContext = new ServerContext();

        // todo write unit test for this
        TestJob testJob = new TestJob(serverContext);
        testJob.start();

        Integer jobId = testJob.getJobId();

        Thread.sleep(3000);

        TestJob runningJob = (TestJob) Job.getRunningJob(jobId);
        runningJob.setSomeData("new data");
        runningJob.updated();


        Thread.sleep(3000);

        runningJob.done();

//lets retrive the result of the job:
        String someData = runningJob.getSomeData();
        System.out.println("result by job: " + someData);


    }
}
