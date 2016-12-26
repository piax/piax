package org.piax.gtrans.async;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.ScheduleEvent;
import org.piax.gtrans.async.Node.NodeMode;

public class EventDispatcher {
    static long vtime = 0;
    public static int nmsgs = 0;
    public static int DEFAULT_MAX_TIME = 200 * 1000;

    static NodeImpl[] nodes;
    static PriorityQueue<Event> timeq = new PriorityQueue<>();
    static Map<String, Count> counter = new HashMap<String, Count>();

    public static class Count {
        int count;
    }

    public static void enqueue(Event ev) {
        ev.vtime = getVTime() + ev.delay;
        timeq.add(ev);
    }

    public static void sched(long delay, Runnable run) {
        enqueue(new ScheduleEvent(null, delay, run));
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
        return vtime;
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

    public static NodeImpl[] getNodes() {
        return nodes;
    }

    public static void run(NodeImpl[] nodes) {
        run(nodes, DEFAULT_MAX_TIME);
    }

    public static void run(NodeImpl[] nodes, long maxTime) {
        EventDispatcher.nodes = nodes;
        boolean isChord = false; //(nodes.length > 0
        // && nodes[0].baseStrategy instanceof ChordStrategy);
        while (true) {
            if (isChord && Sim.isFinished(nodes)) {
                return;
            }
            if (vtime > maxTime) {
                System.out.println("*** execution time over: "
                        + vtime + " > " + maxTime);
                return;
            }
            Event ev = timeq.poll();
            if (ev == null) {
                System.out.println("No more event: time=" + vtime + ", " + nmsgs + " messages");
                return;
            }
            if (vtime < ev.vtime) {
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
            NodeImpl receiver = (NodeImpl)ev.receiver;
            if (receiver != null) {
                addToRoute(ev.routeWithFailed, receiver);
            }
            if (receiver != null && receiver.mode == NodeMode.GRACE) {
                System.out.println(receiver + ": received in grace period: " + ev);
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
