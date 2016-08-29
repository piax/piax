/*
 * PIAXThreadPool.java - A ThreadPool utility class
 *  
 * Copyright (c) 2009-2015 PIAX develoment team
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
 *
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * $Id: PIAXThreadPool.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.util;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.common.PeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PIAXでの用途向けに、チューニング要素の多いThreadPoolExecutorを簡易的に使えるよう、機能制限したクラス。
 * <p>
 * 通常の利用では、スレッドプールで起動するスレッドの最大数と、スレッド名のprefix部分を指定するだけでよい。
 * スレッドプールをshutdownする際にも、shutdownメソッドを呼ぶだけで済むようになっている。
 * executeメソッドについては、通常のメソッドの他、引数にPeerIdを指定するメソッドを用意している。
 * このexecuteメソッドは、スレッド名に指定されたPeerIdを付与する。
 * これにより、log出力の際に、どのPeerで動いたスレッドかを識別できる。
 * <p>
 * PIAXThreadPoolでは、ThreadPoolExecutorの引数として指定するtaskのキューイングに、
 * SynchronousQueueを使っている。これは実質的にキューイングをしないですぐにtaskをスレッド実行することを
 * 意味している。また、拒否されたtaskに対しての振る舞いはデフォルトのAbortPolicy() にしている。
 * この2つの設定（SynchronousQueue＋AbortPolicy()） により、スレッドプールが最大のスレッド数に達した
 * 時点で、新規のtaskは、拒否される。
 * <p>
 * このようにすることで、スレッドプールの動きを簡潔にし、予期せぬデッドロックを防ぐことができる。
 * デッドロックは、次のような場合に起こる。
 * <p>
 * 一連のtask群Tの後にあるtask xが動くような状況で、Tのいくつかのtaskがxの発生により、ロック状態に入り、
 * xが起動または終了することでロック解除されるとする。この場合、スレッドプールは次第に飽和状態になり、
 * 新しいtaskはキューに乗ったままになる。結果デッドロックが起こる。
 * 例えば、互いに相手を呼び合うRPCが再帰的に何段も重なったとすると、末端のRPCがreturnするまで、
 * 他のRPC呼び出しはブロックする。この状態で、末端のすぐにreturnするRPC呼び出しがキューにのってしまうと、
 * 全体がデッドロックしてしまう。
 */
public class PIAXThreadPool extends ThreadPoolExecutor {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(PIAXThreadPool.class);

    /*-- default parameters --*/
    
    /** thread pool内に常時起動しているスレッドの数 */
    public static int CORE_POOL_SIZE = 10;

    /** thread pool内に起動可能なスレッドの最大数 */
    public static int MAX_POOL_SIZE = 2000;
    
    /**
     * アイドル状態のスレッドが終了前に新規タスクを待機する最長時間。
     * この時間経過すると、アイドル状態のスレッドは終了する
     */
    public static long THREAD_KEEP_ALIVE_TIME = 10 * 60 * 1000L;

    /** スレッド名のprefix部分。この後ろにシーケンシャル番号が付く */
    public static String THREAD_NAME_PREFIX = "thPool-";

    /**
     * thread poolの終了時にアクティブなスレッドの終了を待機する最長時間
     */
    public static final long MAX_WAIT_TIME_FOR_TERMINATION = 100L;

    /**
     * PIAXThreadPoolを生成するThreadFactory。
     * スレッド名と、生成したスレッドの優先度を指定するために用いる。
     */
    private static class _ThreadFactory implements ThreadFactory {
        final AtomicInteger thNum = new AtomicInteger(1);
        final String namePrefix;
        final int priority;
        
        _ThreadFactory(String namePrefix, int priority) {
            this.namePrefix = namePrefix;
            this.priority = priority;
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + thNum.getAndIncrement());
            if (t.isDaemon())
                t.setDaemon(false);
            t.setPriority(priority);
            return t;
        }
    }

    /**
     * PIAXThreadPoolをデフォルト設定を使って、生成する。
     */
    public PIAXThreadPool() {
        this(CORE_POOL_SIZE, MAX_POOL_SIZE, THREAD_KEEP_ALIVE_TIME,
                THREAD_NAME_PREFIX, Thread.NORM_PRIORITY);
    }

    /**
     * PIAXThreadPoolを最大スレッド数と、スレッド名のprefixを指定し、生成する。
     * 
     * @param maxPoolSize 最大スレッド数
     * @param threadNamePrefix スレッド名のprefix
     */
    public PIAXThreadPool(int maxPoolSize, String threadNamePrefix) {
        this(CORE_POOL_SIZE, maxPoolSize, THREAD_KEEP_ALIVE_TIME,
                threadNamePrefix, Thread.NORM_PRIORITY);
    }

    /**
     * PIAXThreadPoolを引数の設定に従い、生成する。
     * 
     * @param corePoolSize 最小スレッド数
     * @param maxPoolSize 最大スレッド数
     * @param keepAliveTime アイドルスレッドが停止するまでの最長待ち時間
     * @param threadNamePrefix スレッド名のprefix
     * @param threadPriority スレッドの優先度（1から10の数字）
     */
    public PIAXThreadPool(int corePoolSize, int maxPoolSize, 
            long keepAliveTime, String threadNamePrefix, int threadPriority) {
        super(Math.min(corePoolSize, maxPoolSize), maxPoolSize,
                keepAliveTime, TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(),
                new _ThreadFactory(threadNamePrefix, threadPriority),
                new ThreadPoolExecutor.AbortPolicy());
    }
    
    /**
     * taskをスレッドプールのスレッドを使って実行する。
     * <p>
     * すでに、実行スレッド数が最大数に達した場合は、RejectedExecutionExceptionが発行される。
     * このメソッドを使って実行されたスレッドの名前にはpeerIdが付与される。
     * 
     * @param peerId スレッド名に付与するPeerId
     * @param task 実行するRunnableオブジェクト
     * @throws RejectedExecutionException 実行スレッド数が最大数に達した場合
     */
    public void execute(PeerId peerId, Runnable task)
            throws RejectedExecutionException {
        try {
            super.execute(new ThreadUtil._Runnable(peerId, task));
        } catch (RejectedExecutionException e) {
            if (isShutdown()) {
                logger.info("message discarded as this thread pool is shutdown");
            } else {
                throw e;
            }
        }
    }
    
    /**
     * taskをスレッドプールのスレッドを使って実行する。
     * <p>
     * すでに、実行スレッド数が最大数に達した場合は、RejectedExecutionExceptionが発行される。
     * このメソッドを使った場合、実行されたスレッド名にはpeerIdは付与されない。
     * 
     * @param task 実行するRunnableオブジェクト
     * @throws RejectedExecutionException 実行スレッド数が最大数に達した場合
     */
    @Override
    public void execute(Runnable task) throws RejectedExecutionException {
        try {
            super.execute(task);
        } catch (RejectedExecutionException e) {
            if (isShutdown()) {
                logger.info("message discarded as this thread pool is shutdown");
            } else {
                throw e;
            }
        }
    }

    /**
     * PIAXThreadPoolをshutdownする。
     * この時点でアクティブなスレッドは、一定時間待った上で自動的に終了する。
     * <p>
     * この待ち時間は、MAX_WAIT_TIME_FOR_TERMINATIONの値を変えることで変更できる。
     * 通常はデフォルト値のままでよい。、
     */
    @Override
    public synchronized void shutdown() {
        super.shutdown();
        try {
            super.awaitTermination(MAX_WAIT_TIME_FOR_TERMINATION,
                    TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("some tasks not terminated");
        }
    }
}
