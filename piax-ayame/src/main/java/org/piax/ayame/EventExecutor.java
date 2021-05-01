/*
 * EventExecutor.java - The executor class for Events
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.piax.ayame.Event.TimerEvent;
import org.piax.common.Option.BooleanOption;
import org.piax.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventExecutor {
    private static final Logger logger = LoggerFactory.getLogger(EventExecutor.class);
    // run in real-time
    public static BooleanOption realtime = new BooleanOption(false, "-realtime");
    public static boolean SHOW_PROGRESS = false;

    private static long startTime; // init by reset();
    private static long vtime; // init by reset();
    public static int nmsgs; // init by reset();
    private static int nextEventSerial; // init by reset();
    private static ReentrantLock lock = new ReentrantLock();
    private static Condition cond = lock.newCondition();
    private static PriorityQueue<Event> timeq = new PriorityQueue<>();
    private static Counters counters = new Counters();
    private static boolean terminateExecutor = false;

    static {
        reset();
    }

    public static void load() {
    }

    public static void reset() {
        assert thread == null : "cannot reset while executor is running";
        startTime = System.currentTimeMillis();
        vtime = 0;
        nmsgs = 0;
        nextEventSerial = 0;
        timeq.clear();
        RandomUtil.renewSharedRandom();
        LocalNode.resetLocalNodeMap();
        Node.resetInstances();
    }

    public static void enqueue(Event ev) {
        if (!realtime.value()) {
            //assert ev.vtime != 0;
            ev.serial = nextEventSerial++;
            timeq.add(ev);
        } else {
            lock.lock();
            ev.serial = nextEventSerial++;
            timeq.add(ev);
            cond.signal();
            lock.unlock();
        }
        //System.out.println("enqueued: " + ev);
    }

    public static Event dequeue() throws InterruptedException {
        if (!realtime.value()) {
            return timeq.poll();
        }
        // real-time version
        Event ev;
        lock.lock();
        try {
            while (true) {
                ev = timeq.peek();
                if (ev != null) {
                    long rem = ev.vtime - getVTime();
                    if (rem <= 0) {
                        Event ev0 = timeq.poll();
                        assert ev == ev0;
                        return ev;
                    }
                    cond.await(rem, TimeUnit.MILLISECONDS);
                } else {
                    cond.await();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public static void cancelEvent(Event ev) {
        if (!realtime.value()) {
            timeq.remove(ev);
        } else {
            lock.lock();
            timeq.remove(ev);
            lock.unlock();
        }
    }

    @Deprecated
    public static TimerEvent sched(long delay, Runnable run) {
        return sched(delay, 0, ev -> run.run());
    }

    @Deprecated
    public static TimerEvent sched(long delay, long period, Runnable run) {
        return sched(delay, period, ev -> run.run());
    }

    @Deprecated
    public static TimerEvent sched(long delay, long period, Consumer<TimerEvent> job) {
        return sched(null, delay, period, job);
    }

    public static TimerEvent sched(String name, long delay, Runnable run) {
        return sched(name, delay, 0, ev -> run.run());
    }

    public static TimerEvent sched(String name, long delay, long period,
            Runnable run) {
        return sched(name, delay, period, ev -> run.run());
    }

    public static TimerEvent sched(String name, long delay, long period,
            Consumer<TimerEvent> job) {
        TimerEvent ev = new TimerEvent(name, delay, period, job);
        ev.vtime = getVTime() + delay;
        enqueue(ev);
        return ev;
    }

    public static void runNow(String name, Runnable job) {
        TimerEvent ev = new TimerEvent(name, 0, 0, (dummy) -> job.run());
        ev.vtime = getVTime();
        enqueue(ev);
    }

    public static CompletableFuture<Void> delay(String name, long delay) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        TimerEvent ev = new TimerEvent(name, delay, 0, (dummy) -> {
            future.complete(null);
        });
        ev.vtime = getVTime();
        enqueue(ev);
        return future;
    }

    public static CompletableFuture<Void> runNow(String name) {
        return delay(name, 0);
    }

    public static long getVTime() {
        if (realtime.value()) {
            return System.currentTimeMillis() - startTime;
        } else {
            return vtime;
        }
    }

    @Override
    public String toString() {
        lock.lock();
        String s = "Queue:" + timeq;
        lock.unlock();
        return s;
    }

    /*
     * Consistent Random
     */
    public static Random random() {
        return RandomUtil.getSharedRandom();
    }

    /*
     * Message Counters
     */
    public static void resetMessageCounters() {
        counters.clear();
    }

    public static void dumpMessageCounters() {
        logger.debug("#message count");
        for (Map.Entry<String, Integer> ent : counters.entrySet()) {
            String name = ent.getKey();
            Integer cnt = ent.getValue();
            logger.debug("{}: {}", name, cnt);
        }
    }

    public static void addCounter(String name) {
        counters.add(name, 1);
    }

    public static int getCounter(String name) {
        return counters.get(name);
    }

    /*
     * Event Executor
     */
    private static Thread thread;
    public static synchronized void startExecutorThread() {
        if (thread == null) {
            realtime.set(true);
            terminateExecutor = false;
            thread = new Thread(() -> {
                run(0);
                synchronized (EventExecutor.class) {
                    thread = null;
                }
            });
            thread.start();
        }
    }

    /**
     * request termination of the event executor.
     * 
     * this method blocks until the thread terminates if this method
     * is not invoked from event executor context. 
     */
    public static void terminate() {
        Thread t;
        synchronized (EventExecutor.class) {
            terminateExecutor = true;
            t = thread;
        }
        if (t != null && Thread.currentThread() != t) {
            t.interrupt();
            try {
                t.join();
            } catch (InterruptedException e) {}
        }
    }

    public static void startSimulation(long duration) {
        assert thread == null;
        terminateExecutor = false;
        run(duration);
    }

    private static void run(long duration) {
        logger.debug("Event Executor Started");
        long limit = 0;
        if (duration != 0) {
            limit = getVTime() + duration;
        }
        while (true) {
            if (terminateExecutor) {
                logger.debug("event executor terminated: time={}, {} messages",
                        getVTime(), nmsgs);
                return;
            }
            if (limit != 0 && getVTime() > limit) {
                logger.debug(
                        "*** execution time over: {} > {}", getVTime(), limit);
                return;
            }
            Event ev;
            try {
                ev = dequeue();
            } catch (InterruptedException e) {
                logger.trace("dequeue: interrupted");
                continue;
            }
            if (ev == null) {
                logger.debug("event executor terminated: time={}, {} messages",
                        getVTime(), nmsgs);
                return;
            }
            if (!realtime.value() && vtime < ev.vtime) {
                vtime = ev.vtime;
            }
            if (ev.sender != ev.receiver) {
                nmsgs++;
                if (SHOW_PROGRESS && nmsgs % 10000 == 0) {
                    System.err.println("EventExecutor: " + nmsgs+ " msgs, T="
                            + getVTime());
                }
            }
            addCounter(ev.getType());
            if (logger.isTraceEnabled()) {
                String s;
                if (ev.receiver != null) {
                    s = ev.receiver + " receives " + ev + " from " + ev.sender;
                } else {
                    s = ev.toString();
                }
                logger.trace("-----------------------------------------");
                logger.trace("T{} {}", getVTime(), s);
                if (ev.receiver != null) {
                    logger.trace("{}", ev.receiver.toStringDetail());
                }
            }
            if (ev.receiver != null) {
                LocalNode receiver = null;
                if (ev.receiver instanceof LocalNode) {
                    receiver = (LocalNode) ev.receiver;
                } else {
                    assert ev.receiver.key == null;
                    // wild card case
                    receiver = Node.getAnyLocalNode(ev.receiver.addr);
                    if (receiver == null) {
                        logger.debug("No valid LocalNode: {}", ev);
                        continue;
                    }
                    ev.receiver = receiver;
                }
                receiver.receive(ev);
            } else if (ev.beforeRunHook(null)) {
                ev.run();
            }
        }
    }
}
