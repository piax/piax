/*
 * MyThreadPool.java - A thread pool.
 * 
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ring;

import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * wrapper of ScheduledThreadPoolExecutor
  */
public class MyThreadPool {
    private ScheduledExecutorService pool;

    public MyThreadPool(int nthreads, final String name1, final String name2) {
        pool = Executors.newScheduledThreadPool(nthreads, new ThreadFactory() {
            int count = 1;

            @Override
            public Thread newThread(Runnable r) {
                Thread th = new Thread(r);
                th.setDaemon(true);
                th.setName(name1 + "-" + count + "@" + name2);
                count++;
                return th;
            }
        });
    }

    @Deprecated
    public void schedule(TimerTask task, long delay) {
        throw new UnsupportedOperationException();
    }

    public ScheduledFuture<?> schedule(Runnable task, long delay) {
        ScheduledFuture<?> fs =
                pool.schedule(task, delay, TimeUnit.MILLISECONDS);
        return fs;
    }

    @Deprecated
    public void schedule(TimerTask task, long delay, long period) {
        throw new UnsupportedOperationException();
    }

    public ScheduledFuture<?> schedule(Runnable task, long delay, long period) {
        ScheduledFuture<?> fs =
                pool.scheduleWithFixedDelay(task, delay, period,
                        TimeUnit.MILLISECONDS);
        return fs;
    }

    public void shutdown() {
        pool.shutdownNow();
    }
}
