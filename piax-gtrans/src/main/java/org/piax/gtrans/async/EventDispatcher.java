package org.piax.gtrans.async;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.Event.ScheduleEvent;
import org.piax.gtrans.async.Node.NodeMode;
import org.piax.gtrans.async.Option.BooleanOption;

public class EventDispatcher {
    // run in real-time
    public static BooleanOption realtime = new BooleanOption(false, "-realtime");

    static long vtime = 0;
    public static int nmsgs = 0;
    public static int DEFAULT_MAX_TIME = 200 * 1000;
    static LocalNode[] nodes;
    static ReentrantLock lock = new ReentrantLock();
    static Condition cond = lock.newCondition();
    static PriorityBlockingQueue<Event> timeq = new PriorityBlockingQueue<>();
    static Map<String, Count> counter = new HashMap<String, Count>();
    private static Map<Integer, RequestEvent<?, ?>> requestMap =
            new HashMap<>();

    public static class Count {
        int count;
    }

    public static void load() {
    }

    public static void registerRequestEvent(RequestEvent<?, ?> ev) {
        //System.out.println("register request, id=" + ev.getEventId() + ", " + ev);
        requestMap.put(ev.getEventId(), ev);
    }

    public static RequestEvent<?, ?> lookupRequestEvent(int id) {
        RequestEvent<?, ?> ev = requestMap.remove(id);
        //System.out.println("lookup request, id=" + id + ", " + ev);
        return ev;
    }

    public static void enqueue(Event ev) {
        if (!realtime.value()) {
            timeq.add(ev);
        } else {
            lock.lock();
            timeq.add(ev);
            cond.signal();
            lock.unlock();
        }
        System.out.println("enqueued: " + ev);
    }

    public static Event dequeue() {
        Event ev;
        if (!realtime.value()) {
            try {
                ev = timeq.poll(2000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new Error(e);
            }
            return ev;
        }
        // real-time version
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

    public static void sched(long delay, Runnable run) {
        Event ev = new ScheduleEvent(null, delay, run);
        ev.vtime = getVTime() + delay;
        enqueue(ev);
    }

    public static void reset() {
        vtime = 0;
        nmsgs = 0;
        timeq.clear();
    }

    public static void resetMessageCounters() {
        counter.clear();
    }

    public static long getVTime() {
        if (realtime.value()) {
            return System.currentTimeMillis();
        } else {
            return vtime;
        }
    }

    @Override
    public String toString() {
        return "Queue:" + timeq;
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

    public static void dump() {
        Sim.dump(nodes);
    }

    public static LocalNode[] getNodes() {
        return nodes;
    }

    public static void run(LocalNode[] nodes) {
        run(nodes, DEFAULT_MAX_TIME);
    }

    public static void run(LocalNode[] nodes, long maxTime) {
        EventDispatcher.nodes = nodes;
        boolean isChord = false; //(nodes.length > 0
        // && nodes[0].baseStrategy instanceof ChordStrategy);
        long limit = getVTime() + maxTime;
        while (true) {
            if (isChord && Sim.isFinished(nodes)) {
                return;
            }
            if (getVTime() > limit) {
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
            if (Sim.verbose) {
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
                    Node ins = Node.getInstance(ev.receiver.key);
                    assert ins != null;
                    receiver = (LocalNode) ins;
                    ev.receiver = (LocalNode) ins;
                }
            }
            if (receiver != null) {
                addToRoute(ev.routeWithFailed, receiver);
            }
            if (receiver != null && receiver.mode == NodeMode.GRACE) {
                System.out.println(
                        receiver + ": received in grace period: " + ev);
                if (ev instanceof Lookup) {
                    //post(new LookupError((Lookup)ev, "grace"));
                    addToRoute(ev.route, receiver);
                    ev.run();
                } else if (ev.timeout != null) {
                    receiver.post(new ScheduleEvent(ev.sender,
                            NetworkParams.ONEWAY_DELAY, ev.timeout));
                }
            } else if (receiver != null && (receiver.mode == NodeMode.FAILED
                    || receiver.mode == NodeMode.DELETED)) {
                System.out.println("message received by failed node: " + ev);
                if (ev.timeout != null) {
                    receiver.post(new ScheduleEvent(ev.sender,
                            NetworkParams.NETWORK_TIMEOUT - ev.delay,
                            ev.timeout));
                }
            } else {
                addToRoute(ev.route, receiver);
                ev.run();
            }
        }
    }

    private static void addToRoute(List<Node> route, Node next) {
        if (route.isEmpty() || route.get(route.size() - 1) != next) {
            route.add(next);
        }
    }

}
