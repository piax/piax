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

public class EventExecutor {
    // run in real-time
    public static BooleanOption realtime = new BooleanOption(false, "-realtime");
    public static boolean REALWORLD = false;

    private static long startTime = System.currentTimeMillis();
    private static long vtime = 0;
    public static int nmsgs = 0;
    public static int DEFAULT_MAX_TIME = 200 * 1000;
    private static ReentrantLock lock = new ReentrantLock();
    private static Condition cond = lock.newCondition();
    private static PriorityQueue<Event> timeq = new PriorityQueue<>();
    private static LatencyProvider latencyProvider;
    private static Map<String, Count> counter = new HashMap<String, Count>();

    // consistent random generator
    private static Random rand = new MersenneTwister();

    public static class Count {
        int count;
    }

    public static void load() {
    }

    public static void enqueue(Event ev) {
        if (!realtime.value()) {
            assert ev.vtime != 0;
            timeq.add(ev);
        } else {
            lock.lock();
            timeq.add(ev);
            cond.signal();
            lock.unlock();
        }
        //System.out.println("enqueued: " + ev);
    }

    public static Event dequeue() {
        if (!realtime.value()) {
            return timeq.poll();
        }
        // real-time version
        Event ev;
        lock.lock();
        final int GRACE = 2000;
        try {
            while (true) {
                ev = timeq.peek();
                long rem = GRACE;
                if (ev != null) {
                    rem = ev.vtime - getVTime();
                    if (rem <= 0) {
                        Event ev0 = timeq.poll();
                        assert ev == ev0;
                        return ev;
                    }
                }
                //System.out.println("Queue: " + timeq);
                try {
                    boolean rc = cond.await(rem, TimeUnit.MILLISECONDS);
                    if (!rc && rem == GRACE) {
                        return null;
                    }
                } catch (InterruptedException e) {
                    throw new Error(e);
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

    public static void reset() {
        vtime = 0;
        nmsgs = 0;
        timeq.clear();
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
        System.out.println("#message count");
        for (Map.Entry<String, Count> ent : counter.entrySet()) {
            String name = ent.getKey();
            Count cnt = ent.getValue();
            System.out.println(name + ": " + cnt.count);
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
    public static void startExecutorThread() {
        synchronized (EventExecutor.class) {
            if (thread == null) {
                realtime.set(true);
                thread = new Thread(() -> run(0));
                thread.start();
            }
        }
    }

    public static void startSimulation(long duration) {
        assert thread == null;
        run(duration);
    }

    private static void run(long duration) {
        System.out.println("Event Executor Started");
        long limit = 0;
        if (duration != 0) {
            limit = getVTime() + duration;
        }
        while (true) {
            if (limit != 0 && getVTime() > limit) {
                System.out.println(
                        "*** execution time over: " + getVTime() + " > " + limit);
                return;
            }
            Event ev = dequeue();
            if (ev == null) {
                System.out.println("No more event: time=" + vtime + ", " + nmsgs
                        + " messages");
                return;
            }
            if (!realtime.value() && vtime < ev.vtime) {
                vtime = ev.vtime;
            }
            if (ev.sender != ev.receiver) {
                nmsgs++;
            }
            addCounter(ev.getType());
            if (Log.verbose) {
                String s;
                if (ev.receiver != null) {
                    s = ev.receiver + " receives " + ev + " from " + ev.sender;
                } else {
                    s = ev.toString();
                }
                System.out.println("-----------------------------------------");
                System.out.println("T" + getVTime() + " " + s);
                if (ev.receiver != null) {
                    System.out.println(ev.receiver.toStringDetail());
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
                        System.out.println("No valid LocalNode: " + ev);
                        continue;
                    }
                    ev.receiver = receiver;
                }
            }
            if (receiver != null) {
                addToRoute(ev.routeWithFailed, receiver);
            }
            if (receiver != null && receiver.mode == NodeMode.GRACE) {
                System.out.println(
                        receiver + ": received in grace period: " + ev);
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
                    || receiver.mode == NodeMode.DELETED)) {
                System.out.println("message received by deleted or failed node: " + ev);
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

    public static int latency(Node a, Node b) {
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
