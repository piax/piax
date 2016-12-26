package org.piax.gtrans.async;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.async.Event.ScheduleEvent;
import org.piax.gtrans.async.Sim.LookupStat;

public class NodeImpl extends Node implements NodeListener {
    public static EventDispatcher dispatcher = new EventDispatcher();
    public long insertionStartTime = -1L;
    public long insertionEndTime;

    public Node introducer;
    public Node succ, pred;
    public NodeMode mode = NodeMode.OUT;
    public NodeStrategy baseStrategy;
    public final NodeStrategy topStrategy;
    private NodeEventCallback joinCallback;
    private LinkChangeEventCallback predChange;
    private LinkChangeEventCallback succChange;

    public NodeImpl(NodeStrategy topStrategy, int latency, int id) {
        super(latency, id);
        this.topStrategy = topStrategy;
        this.baseStrategy = topStrategy;
        topStrategy.setupNode(this);
        topStrategy.setupListener(this);
    }
    
    public void setBaseStrategy(NodeStrategy strategy) {
        this.baseStrategy = strategy;
        strategy.setupNode(this);
    }

    public void setLinkChangeEventHandler(LinkChangeEventCallback predChange,
            LinkChangeEventCallback succChange) {
        this.predChange = predChange;
        this.succChange = succChange;
    }

    @Override
    public String toString() {
        return "N" + id;
    }

    public static void verbose(String s) {
        if (Sim.verbose) {
            System.out.println(s);
        }
    }

    public void setPred(Node newPred) {
        Node old = pred;
        pred = newPred;
        if (pred != null && this.predChange != null) {
            this.predChange.run(old, newPred);
        }
    }

    public void setSucc(Node newSucc) {
        Node old = succ;
        succ = newSucc;
        if (pred != null && this.predChange != null) {
            this.succChange.run(old, newSucc);
        }
    }


    /**
     * post a event
     * @param ev
     */
    public void post(Event ev) {
        post(ev, null);
    }

    public void post(Event ev, Runnable timeout) {
        ev.sender = ev.origin = this;
        ev.route.add(this);
        if (ev.routeWithFailed.size() == 0) {
            ev.routeWithFailed.add(this);
        }
        if (ev.delay == Node.NETWORK_LATENCY) {
            ev.delay = latency(ev.receiver);
        }
        ev.timeout = timeout;
        EventDispatcher.enqueue(ev);
        if (Sim.verbose) {
            System.out.println(this + "|send event " + ev + ", (arrive at T"
                    + ev.vtime + ")");
        }
    }
    /**
     * post an Event to a node
     * @param dest
     * @param ev
     */
    public void forward(Node dest, Event ev) {
        this.forward(dest, ev, null, null);
    }

    public void forward(Node dest, Event ev, Runnable timeout) {
        this.forward(dest, ev, timeout, null);
    }

    public void forward(Node dest, Event ev, Runnable timeout, Runnable error) {
        assert ev.origin != null;
        ev.sender = this;
        ev.timeout = timeout;
        ev.error = error; // overwrite!
        if (ev.delay == Node.NETWORK_LATENCY) {
            ev.delay = latency(dest);
        }
        EventDispatcher.enqueue(ev);
        if (Sim.verbose) {
            System.out.println(this + "|forward to " + dest + ", " + ev
                    + ", (arrive at T" + ev.vtime + ")");
        }
        ev.receiver = dest;
    }


    long getVTime() {
        return EventDispatcher.getVTime();
    }

    @Override
    public void nodeInserted() {
        insertionEndTime = getVTime();
        mode = NodeMode.INSERTED;
        if (this.joinCallback != null) {
            this.joinCallback.run(this);
        }
    }

    public long getInsertionTime() {
        if (insertionEndTime == 0) {
            throw new Error("not inserted");
        }
        return insertionEndTime - insertionStartTime;
    }
    
    public int getMessages4Join() {
        return topStrategy.getMessages4Join();
    }

    /**
     * insert an initial node
     */
    public void initInitialNode() {
        topStrategy.initInitialNode();
        mode = NodeMode.INSERTED;
        insertionStartTime = insertionEndTime = getVTime();
    }

    /**
     * locate the node position and insert
     * @param introducer
     */
    public void joinUsingIntroducer(Node introducer) {
        joinUsingIntroducer(introducer, this.joinCallback);
    }

    public void joinLater(NodeImpl introducer, long delay, NodeEventCallback callback) {
        mode = NodeMode.TO_BE_INSERTED;
        if (delay == 0) {
            joinUsingIntroducer(introducer, callback);
        } else {
            post(new ScheduleEvent(this, delay, () -> {
                joinUsingIntroducer(introducer, callback);
            }));
        }
    }

    /**
     * locate the node position and insert
     * @param introducer
     * @param callback  a callback that is called after join succeeds
     */
    public void joinUsingIntroducer(Node introducer, NodeEventCallback callback) {
        if (insertionStartTime == -1) {
            insertionStartTime = getVTime();
        }
        this.mode = NodeMode.INSERTING;
        this.introducer = introducer;
        this.joinCallback = callback;
        Event ev = new Lookup(introducer, id, this, (LookupDone results) -> {
            topStrategy.joinAfterLookup(results);
        });
        post(ev);
    }

    public void leave() {
        leave(null);
    }

    public void leave(NodeEventCallback callback) {
        System.out.println("Node " + this + " leaves");
        topStrategy.leave(callback);
    }

    public void fail() {
        System.out.println("Node " + this + " fails");
        this.mode = NodeMode.FAILED;
    }

    /**
     * process a lookup event
     * @param lookup
     */
    public void handleLookup(Lookup lookup) {
        topStrategy.handleLookup(lookup);
    }

    /**
     * keyを検索し，統計情報を stat に追加する．
     * 
     * @param key
     * @param stat
     */
    public void lookup(int key, LookupStat stat) {
        System.out.println(this + " lookup " + key);
        long start = dispatcher.getVTime();
        post(new Lookup(this, key, this, (LookupDone done) -> {
            if (done.req.id != done.pred.id) {
                System.out.println("Lookup error: req.id=" + done.req.id + ", " + done.pred.id);
                System.out.println(done.pred.toStringDetail());
                //dispatcher.dump();
                stat.lookupFailure.addSample(1);
            } else {
                stat.lookupFailure.addSample(0);
            }
            // 先頭と末尾は検索開始ノードなので2を減じる．
            // ただし，検索開始ノード＝検索対象ノードの場合 0 ホップ
            int h = Math.max(done.routeWithFailed.size() - 2, 0);
            stat.hops.addSample(h);
            int nfails = done.routeWithFailed.size() - done.route.size();
            //stat.failedNodes.addSample(nfails);
            stat.failedNodes.addSample(nfails > 0 ? 1 : 0);
            long end = dispatcher.getVTime();
            long elapsed = (int)(end - start);
            stat.time.addSample((double)elapsed);
            if (nfails > 0) {
                System.out.println("lookup done!: "
                        + done.route
                        + " (" + h + " hops, " + elapsed
                        + ", actual route=" + done.routeWithFailed
                        + ", evid=" + done.req.getEventId()
                        + ")");
            } else {
                System.out.println("lookup done: " + done.route
                        + " (" + h + " hops, " + elapsed + ")");
            }
        }));
    }

    public NodeAndIndex getClosestPredecessor(int key) {
        Comparator<NodeAndIndex> comp = (NodeAndIndex a, NodeAndIndex b) -> {
            // aの方がkeyに近ければ正の数，bの方がkeyに近ければ負の数を返す
            int ax = a.node.id - key;
            int bx = b.node.id - key;
            if (ax == bx) {
                return 0;
            } else if (ax == 0) {
                return +1;
            } else if (bx == 0) {
                return -1;
            } else if (Integer.signum(ax) == Integer.signum(bx)) {
                return ax - bx;
            } else if (ax > 0) {
                return -1;
            } else {
                return +1;
            }
        };
        /*Comparator<Node> compx = (Node a, Node b) -> {
            int rc = comp.compare(a, b);
            System.out.println("compare " + a.id + " with " + b.id + " -> " + rc);
            return rc;
        };*/
        List<NodeAndIndex> nodes = topStrategy.getAllLinks2();
        //Collections.sort(nodes, comp);
        //System.out.println("nodes = " + nodes);
        Optional<NodeAndIndex> n = nodes.stream().max(comp);
        //System.out.println("key = " + key);
        //System.out.println("max = " + n);
        return n.orElse(null);
    }
}
