package org.piax.gtrans.async;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.piax.gtrans.async.Event.ErrorEvent;
import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.Event.TimerEvent;
import org.piax.gtrans.async.EventException.GraceStateException;
import org.piax.gtrans.async.Node.NodeMode;
import org.piax.gtrans.async.Option.BooleanOption;
import org.piax.util.MersenneTwister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventExecutor {
    private static final Logger logger = LoggerFactory.getLogger(EventExecutor.class);
    // run in real-time
    public static BooleanOption realtime = new BooleanOption(false, "-realtime");
    public static boolean REALWORLD = false;

    private static long startTime; // init by reset();
    private static long vtime; // init by reset();
    public static int nmsgs; // init by reset();
    private static int eventCount; // init by reset();
    private static ReentrantLock lock = new ReentrantLock();
    private static Condition cond = lock.newCondition();
    private static PriorityQueue<Event> timeq = new PriorityQueue<>();
    private static LatencyProvider latencyProvider;
    private static Map<String, Count> counter = new HashMap<String, Count>();
    private static boolean terminateExecutor = false;

    // consistent random generator
    private static Random rand = new MersenneTwister();

    public static class Count {
        int count;
    }

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
        eventCount = 0;
        timeq.clear();
        Node.resetInstances();
    }

    public static void enqueue(Event ev) {
        if (!realtime.value()) {
            assert ev.vtime != 0;
            ev.serial = eventCount++;
            timeq.add(ev);
        } else {
            lock.lock();
            ev.serial = eventCount++;
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

    public static TimerEvent sched(long delay, Runnable run) {
        return sched(delay, 0, ev -> run.run());
    }

    public static TimerEvent sched(long delay, long period, Runnable run) {
        return sched(delay, period, ev -> run.run());
    }

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
        return rand;
    }

    public static void setRandom(Random rand) {
        EventExecutor.rand = rand;
    }
    
    /*
     * Message Counters
     */
    public static void resetMessageCounters() {
        counter.clear();
    }

    public static void dumpMessageCounters() {
        logger.debug("#message count");
        for (Map.Entry<String, Count> ent : counter.entrySet()) {
            String name = ent.getKey();
            Count cnt = ent.getValue();
            logger.debug("{}: {}", name, cnt.count);
        }
    }

    public static void addCounter(String name) {
        Count cnt = counter.get(name);
        if (cnt == null) {
            cnt = new Count();
            counter.put(name, cnt);
        }
        cnt.count++;
    }

    public static int getCounter(String name) {
        Count cnt = counter.get(name);
        if (cnt == null) {
            return 0;
        }
        return cnt.count;
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
                logger.debug("*** event executor terminated: {}",
                		getVTime());
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
                    logger.trace(ev.receiver.toStringDetail());
                }
                //System.out.println("so far:" + ev.route);
            }
            LocalNode receiver = null;
            if (ev.receiver != null) {
                if (ev.receiver instanceof LocalNode) {
                    receiver = (LocalNode) ev.receiver;
                } else {
                    assert ev.receiver.key == null;
                    // special case
                    receiver = Node.getAnyLocalNode();
                    if (receiver == null) {
                        logger.debug("No valid LocalNode: {}", ev);
                        continue;
                    }
                    ev.receiver = receiver;
                }
            }
            if (receiver != null) {
                addToRoute(ev.routeWithFailed, receiver);
            }
            if (receiver != null && receiver.mode == NodeMode.GRACE) {
                logger.debug(
                        "{}: received in grace period: {}", receiver, ev);
                if (ev instanceof Lookup) {
                    addToRoute(ev.route, receiver);
                    if (ev.beforeRunHook(receiver)) {
                        ev.run();
                    }
                } else if (ev instanceof RequestEvent) {
                    receiver.post(new ErrorEvent((RequestEvent<?, ?>)ev, 
                            new GraceStateException()));
                }
            } else if (receiver != null && (receiver.isFailed()
                    || receiver.mode == NodeMode.OUT
                    || receiver.mode == NodeMode.DELETED)) {
                logger.trace("message received by not inserted or failed node: {}", ev);
            } else {
                addToRoute(ev.route, receiver);
                if (ev.beforeRunHook(receiver)) {
                    ev.run();
                }
            }
        }
    }

    private static void addToRoute(List<Node> route, Node next) {
        if (route.isEmpty() || route.get(route.size() - 1) != next) {
            route.add(next);
        }
    }

    /*
     * Latency Management
     */
    public static void setLatencyProvider(LatencyProvider p) {
        latencyProvider = p;
    }

    public static long latency(Node a, Node b) {
        if (EventExecutor.realtime.value()) {
            return 0;
        }
        if (a == b) {
            return 0;
        }
        if (latencyProvider == null) {
            return 100;
        }
        return latencyProvider.latency(a, b);
    }
}
