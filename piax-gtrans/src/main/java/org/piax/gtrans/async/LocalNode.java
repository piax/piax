package org.piax.gtrans.async;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.piax.common.Endpoint;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.async.EventException.RPCEventException;
import org.piax.gtrans.async.EventSender.EventSenderNet;
import org.piax.gtrans.async.EventSender.EventSenderSim;
import org.piax.gtrans.async.ObjectLatch.WrappedException;
import org.piax.gtrans.async.Sim.LookupStat;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.util.UniqId;

public class LocalNode extends Node {
    public long insertionStartTime = -1L;
    public long insertionEndTime;

    final EventSender sender;
    public Node introducer;
    public Node succ, pred;
    public NodeMode mode = NodeMode.OUT;
    public NodeStrategy baseStrategy;
    public final NodeStrategy topStrategy;
    private LinkChangeEventCallback predChange;
    private LinkChangeEventCallback succChange;

    public static LocalNode newLocalNode(TransportId transId,
            ChannelTransport<?> trans, Comparable<?> rawkey,
            NodeStrategy topStrategy, int latency)
            throws IdConflictException, IOException {
        DdllKey ddllkey = new DdllKey(rawkey, new UniqId(trans.getPeerId()));
        LocalNode node =
                new LocalNode(transId, trans, ddllkey, topStrategy, latency);
        return node;
    }

    public LocalNode(TransportId transId, ChannelTransport<?> trans,
            DdllKey ddllkey, NodeStrategy topStrategy, int latency)
            throws IdConflictException, IOException {
        super(ddllkey, trans == null ? null : trans.getEndpoint(), latency);
        if (trans == null) {
            this.sender = EventSenderSim.getInstance();
        } else {
            try {
                this.sender = new EventSenderNet(transId, trans);
            } catch (IdConflictException | IOException e) {
                throw e;
            }
        }
        this.topStrategy = topStrategy;
        this.baseStrategy = topStrategy;
        topStrategy.setupNode(this);
    }

    /**
     * replace this instance with corresponding Node object on serialization.
     * 
     * @return
     * @throws ObjectStreamException
     */
    private Object writeReplace() {
        Node repl = new Node(this.key, this.addr, this.latency);
        return repl;
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

    public static void verbose(String s) {
        if (Sim.verbose) {
            System.out.println(s);
        }
    }

    @Override
    public String toStringDetail() {
        return this.topStrategy.toStringDetail();
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

    public void post(Event ev, FailureCallback failure) {
        ev.sender = ev.origin = this;
        ev.route.add(this);
        if (ev.routeWithFailed.size() == 0) {
            ev.routeWithFailed.add(this);
        }
        if (ev.delay == Node.NETWORK_LATENCY) {
            ev.delay = latency(ev.receiver);
        }
        ev.failureCallback = failure;
        ev.vtime = EventDispatcher.getVTime() + ev.delay;
        if (Sim.verbose) {
            if (ev.delay != 0) {
                System.out.println(this + "|send event " + ev + ", (arrive at T"
                        + ev.vtime + ")");
            } else {
                System.out.println(this + "|send event " + ev);
            }
        }
        try {
            sender.send(ev);
        } catch (RPCException e) {
            verbose(this + " got exception: " + e);
            failure.run(new RPCEventException(e));
        }
    }

    /**
     * post an Event to a node
     * @param dest
     * @param ev
     */
    public void forward(Node dest, Event ev) {
        this.forward(dest, ev, null);
    }

    public void forward(Node dest, Event ev, FailureCallback failure) {
        assert ev.origin != null;
        ev.sender = this;
        ev.failureCallback = failure;
        if (ev.delay == Node.NETWORK_LATENCY) {
            ev.delay = latency(dest);
        }
        ev.receiver = dest;
        if (Sim.verbose) {
            if (ev.delay != 0) {
                System.out.println(this + "|forward to " + dest + ", " + ev
                        + ", (arrive at T" + ev.vtime + ")");
            } else {
                System.out.println(this + "|forward to " + dest + ", " + ev);
            }
        }
        try {
            sender.forward(ev);
        } catch (RPCException e) {
            verbose(this + " got exception: " + e);
            failure.run(new RPCEventException(e));
        }
    }

    long getVTime() {
        return EventDispatcher.getVTime();
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

    /*
     * synchronous interface
     */
    /**
     * insert a key into a ring.
     * 
     * @param introducer a node that has been inserted to the ring.
     * @return true on success
     * @throws IOException thrown in communication errors
     */
    public boolean addKey(Endpoint introducer) throws IOException,
        InterruptedException {
        Node temp = Node.getTemporaryInstance(introducer);
        ObjectLatch<Boolean> latch = new ObjectLatch<>(1);
        //System.out.println(this + ": joining");
        joinUsingIntroducer(temp, () -> latch.set(true),
                e -> latch.setException(e));
        try {
            return latch.getOrException();
        } catch (InterruptedException e) {
            throw e;
        } catch (WrappedException e) {
            throw new IOException(e.getCause());
        } finally {
            //System.out.println("join finished");
        }
    }

    public boolean removeKey() throws InterruptedException {
        ObjectLatch<Boolean> latch = new ObjectLatch<>(1);
        System.out.println(this + ": leaving");
        leave(() -> latch.set(true));
        try {
            return latch.getOrException();
        } catch (InterruptedException e) {
            throw e;
        } catch (WrappedException e) {
            System.out.println(this + ": leave got " + e);
            return false;
        } finally {
            System.out.println("leave finished");
        }
    }

    /*
     * asynchronous interface
     */

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
    public void joinLater(LocalNode introducer, long delay,
            Runnable callback) {
        joinLater(introducer, delay, callback, (exc) -> {
            throw new Error("joinLater got exception", exc);
        });
    }

    public void joinLater(LocalNode introducer, long delay,
            Runnable callback, FailureCallback failure) {
        mode = NodeMode.TO_BE_INSERTED;
        if (delay == 0) {
            joinUsingIntroducer(introducer, callback, failure);
        } else {
            EventDispatcher.sched(delay, () -> {
                this.joinUsingIntroducer(introducer, callback, failure);
            });
        }
    }

    public void joinUsingIntroducer(Node introducer, Runnable success) {
        joinUsingIntroducer(introducer, success, exc -> {
            throw new Error("joinUsingIntroducer got exception", exc);
        });
    }

    /**
     * locate the node position and insert
     * @param introducer
     * @param callback  a callback that is called after join succeeds
     */
    public void joinUsingIntroducer(Node introducer, Runnable success,
            FailureCallback failure) {
        if (insertionStartTime == -1) {
            insertionStartTime = getVTime();
        }
        this.mode = NodeMode.INSERTING;
        this.introducer = introducer;
        Event ev = new Lookup(introducer, key, this, (LookupDone results) -> {
            topStrategy.joinAfterLookup(results, () -> {
                insertionEndTime = getVTime();
                mode = NodeMode.INSERTED;
                success.run();
            }, failure);
        });
        post(ev, failure);
    }

    public void leave() {
        leave(null);
    }

    public void leave(Runnable callback) {
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
    public void lookup(DdllKey key, LookupStat stat) {
        System.out.println(this + " lookup " + key);
        long start = EventDispatcher.getVTime();
        post(new Lookup(this, key, this, (LookupDone done) -> {
            if (done.req.key.compareTo(done.pred.key) != 0) {
                System.out.println("Lookup error: req.key=" + done.req.key
                        + ", " + done.pred.key);
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
            long end = EventDispatcher.getVTime();
            long elapsed = (int) (end - start);
            stat.time.addSample((double) elapsed);
            if (nfails > 0) {
                System.out.println("lookup done!: " + done.route + " (" + h
                        + " hops, " + elapsed + ", actual route="
                        + done.routeWithFailed + ", evid="
                        + done.req.getEventId() + ")");
            } else {
                System.out.println("lookup done: " + done.route + " (" + h
                        + " hops, " + elapsed + ")");
            }
        }));
    }

    public NodeAndIndex getClosestPredecessor(DdllKey key) {
        Comparator<NodeAndIndex> comp = (NodeAndIndex a, NodeAndIndex b) -> {
            // aの方がkeyに近ければ正の数，bの方がkeyに近ければ負の数を返す
            // [a, key, b) -> plus
            // [b, key, a) -> minus
            if (Node.isOrdered(a.node.key, true, key, b.node.key, false)) {
                return +1;
            }
            return -1;
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
