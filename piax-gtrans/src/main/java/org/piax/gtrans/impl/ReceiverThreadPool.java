/*
 * ReceiverThreadPool.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: ReceiverThreadPool.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import java.util.ArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.gtrans.GTransConfigValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ReceiverThreadPool {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(ReceiverThreadPool.class);
    
    /*-- tuning parameters of the thread pooling --*/
    
    static int CORE_POOL_SIZE = 10;
    static String THREAD_NAME_PREFIX = "thPool-";
    
    /**
     * アイドル状態のスレッドが終了前に新規タスクを待機する最大時間
     */
    public static long THREAD_KEEP_ALIVE_TIME = 10 * 60 * 1000L;

    /**
     * スレッドプールの終了時にアクティブなスレッドの終了を待機する最長時間
     */
    public static final long MAX_WAIT_TIME_FOR_TERMINATION = 100L;
    
    private static class ThreadPoolFactory implements ThreadFactory {
        final AtomicInteger thNum = new AtomicInteger(1);

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, THREAD_NAME_PREFIX + thNum.getAndIncrement());
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
    
    private static ThreadPoolExecutor staticThreadPool;
    private static ArrayList<ThreadPoolExecutor> threadPoolList;
    static {
        threadPoolList = new ArrayList<ThreadPoolExecutor>();
    }
    private ThreadPoolExecutor threadPool;
    public static boolean threadPoolByInstance = false;
    /** スレッドプールを適切にfinさせるための参照カウンタ */
    private static int instanceCount = 0;

    public static synchronized int getActiveCount() {
        int n = 0;
        if (threadPoolList.size() > 0) {
            for (ThreadPoolExecutor te : threadPoolList) {
                n += te.getActiveCount();
            }
        }
        return n;
    }
    
    /**
     * ReceiverMgrオブジェクトを起動する。
     */
    public ReceiverThreadPool() {
        synchronized (this) {
            if (threadPoolByInstance) {
                    threadPool = new ThreadPoolExecutor(
                            Math.min(CORE_POOL_SIZE,
                                    GTransConfigValues.MAX_RECEIVER_THREAD_SIZE), 
                            GTransConfigValues.MAX_RECEIVER_THREAD_SIZE,
                            THREAD_KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS,
                            new SynchronousQueue<Runnable>(), 
                            new ThreadPoolFactory(),
                            new ThreadPoolExecutor.AbortPolicy());
                    threadPoolList.add(threadPool);
            } else {
                if (instanceCount == 0) {
                    staticThreadPool = new ThreadPoolExecutor(
                            Math.min(CORE_POOL_SIZE,
                                    GTransConfigValues.MAX_RECEIVER_THREAD_SIZE), 
                            GTransConfigValues.MAX_RECEIVER_THREAD_SIZE,
                            THREAD_KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS,
                            new SynchronousQueue<Runnable>(), 
                            /*
                             * 10/12/8 BlockingQueueを使うことによるデッドロックを発見
                             * SynchronousQueue に切り替える
                             */
                            new ThreadPoolFactory(),
                            new ThreadPoolExecutor.AbortPolicy());
                    threadPoolList.add(staticThreadPool);
                }
                threadPool = staticThreadPool;
            }
            instanceCount++;
        }
    }
    
    public void execute(Runnable receiveTask) throws RejectedExecutionException {
        try {
            threadPool.execute(receiveTask);
        } catch (RejectedExecutionException e) {
            if (threadPool.isShutdown()) {
                logger.info("message discarded as threadPool is shutdown");
            } else {
                throw e;
            }
        }
    }
    
    /**
     * ReceiverMgrオブジェクトを終了させる。
     * スレッドプールがこのメソッドにより終了する。
     */
    public synchronized void fin() {
        instanceCount--;
        
        if (threadPoolByInstance) {
            // shutdown thread pool
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(
                        MAX_WAIT_TIME_FOR_TERMINATION, 
                        TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warn("some tasks not terminated");
            }
            threadPoolList.remove(threadPool);
        } else {
            // 最後のインスタンスを終了させる場合は、thread poolを終わらせる
            // 必要がある。
            if (instanceCount == 0) {
                // shutdown thread pool
                staticThreadPool.shutdown();
                try {
                    staticThreadPool.awaitTermination(
                            MAX_WAIT_TIME_FOR_TERMINATION, 
                            TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.warn("some tasks not terminated");
                }
                threadPoolList.remove(staticThreadPool);
            }
        }
    }
}
