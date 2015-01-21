package edu.vt;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CorrectnessTester {
    private final ExecutorService es;

    public CorrectnessTester(int threadCount) {
        es = Executors.newFixedThreadPool(threadCount); // create thread pool
    }

    public void start(Runnable[] tasks) {
        for (Runnable task : tasks)
            es.execute(task);
    }

    public void awaitTermination() throws InterruptedException {
        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }
}
