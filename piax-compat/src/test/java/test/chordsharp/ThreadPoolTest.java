package test.chordsharp;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolTest {
    ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);
    long startTime;

    long time() {
        return System.currentTimeMillis() - startTime;
    }

    class Task implements Runnable {
        AtomicInteger count = new AtomicInteger(0);

        public void run() {
            int c = count.getAndIncrement();
            System.out.println("start " + c + ": " + time());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("interrupted");
            }
            System.out.println("end " + time());
        }
    }

    void test() {
        Runnable task = new Task();
        ScheduledFuture<?> fs1 =
                pool.schedule(task, 100, TimeUnit.MILLISECONDS);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
        System.out.println("cancel the 1st");
        fs1.cancel(false);
        System.out.println("isDone = " + fs1.isDone());
        try {
            fs1.get();
        } catch (Exception e) {
            System.out.println("get() got: " + e);
        }
        ScheduledFuture<?> fs2 =
                pool.schedule(task, 100, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) {
        ThreadPoolTest test = new ThreadPoolTest();
        test.startTime = System.currentTimeMillis();
        test.test();
    }

}
