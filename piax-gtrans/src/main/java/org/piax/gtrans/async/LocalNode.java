package org.piax.gtrans.async;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.piax.common.Endpoint;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.EventException.RPCEventException;
import org.piax.gtrans.async.EventException.RetriableException;
import org.piax.gtrans.async.EventSender.EventSenderNet;
import org.piax.gtrans.async.EventSender.EventSenderSim;
import org.piax.gtrans.async.Sim.LookupStat;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.util.UniqId;

public class LocalNode extends Node {
    public static final int INSERTION_DELETION_RETRY = 10; 
    public long insertionStartTime = -1L;
    public long insertionEndTime;

    final EventSender sender;
    public Node introducer;
    public Node succ, pred;
    public NodeMode mode = NodeMode.OUT;
    private boolean isFailed = false;   // for simulation
    public NodeStrategy baseStrategy;
    public final NodeStrategy topStrategy;
    private LinkChangeEventCallback predChange;
    private LinkChangeEventCallback succChange;

    // incomplete requests 
    Map<Integer, RequestEvent<?, ?>> ongoingRequests = new HashMap<>();
    // requests that are not ack'ed
    Map<Integer, RequestEvent<?, ?>> unAckedEvents = new HashMap<>();

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
    public String toString() {
        return super.toString() + (isFailed ? "(failed)" : ""); 
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
        if (ev instanceof RequestEvent && failure == null) {
            RequestEvent<?, ?> req = (RequestEvent<?, ?>)ev;
            failure = exc -> req.future.completeExceptionally(exc);
        }
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
        ev.beforeSendHook(this);
        if (!isFailed) {
            try {
                sender.send(ev);
            } catch (RPCException e) {
                verbose(this + " got exception: " + e);
                failure.run(new RPCEventException(e));
            }
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
        ev.beforeForwardHook(this);
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
        if (!isFailed()) {
            try {
                sender.forward(ev);
            } catch (RPCException e) {
                verbose(this + " got exception: " + e);
                failure.run(new RPCEventException(e));
            }
        }
    }

    private long getVTime() {
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

    /**
     * insert a key into a ring.
     * 
     * @param introducer a node that has been inserted to the ring.
     * @return true on success
     * @throws IOException thrown in communication errors
     */
    public boolean addKey(Endpoint introducer) throws IOException,
        InterruptedException {
        System.out.println(this + ": addKey");
        Node temp = Node.getTemporaryInstance(introducer);
        CompletableFuture<Boolean> future = joinAsync(temp);
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        } catch (InterruptedException e) {
            throw e;
        }
    }

    public boolean removeKey() throws IOException, InterruptedException {
        System.out.println(this + ": removeKey");
        CompletableFuture<Boolean> future = leaveAsync();
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        } catch (InterruptedException e) {
            throw e;
        }
    }

    /**
     * insert an initial node
     */
    public void joinInitialNode() {
        topStrategy.initInitialNode();
        mode = NodeMode.INSERTED;
        insertionStartTime = insertionEndTime = getVTime();
    }

    /**
     * locate the node position and insert
     * @param introducer
     * @param success  a callback that is called after join succeeds
     */
    public CompletableFuture<Boolean> joinAsync(Node introducer) { 
        CompletableFuture<Boolean> joinFuture = new CompletableFuture<>();
        joinAsync(introducer, INSERTION_DELETION_RETRY, joinFuture);
        return joinFuture;
    }

    /**
     * locate the node position and insert
     * @param introducer
     * @param count      number of remaining retries
     * @param joinFuture
     */
    private void joinAsync(Node introducer, int count,
            CompletableFuture<Boolean> joinFuture) {
        if (insertionStartTime == -1) {
            insertionStartTime = getVTime();
        }
        this.mode = NodeMode.INSERTING;
        this.introducer = introducer;
        CompletableFuture<LookupDone> lookupFuture = new CompletableFuture<>();
        Event ev = new Lookup(introducer, key, this, lookupFuture);
        lookupFuture.handle((results, exc) -> {
            if (exc != null) {
                joinFuture.completeExceptionally(exc);
            } else {
                CompletableFuture<Boolean> future = new CompletableFuture<>();
                topStrategy.joinAfterLookup(results, future);
                future.handle((rc, exc2) -> {
                    if (exc2 != null) {
                        verbose(this + ": joinAfterLookup failed:" + exc2
                                + ", count=" + count);
                        mode = NodeMode.OUT;
                        // reset insertionStartTime ?
                        if (exc2 instanceof RetriableException && count > 1) {
                            joinAsync(introducer, count - 1, joinFuture);
                        } else {
                            joinFuture.completeExceptionally(exc2);
                        }
                        return false;
                    }
                    if (rc) {
                        insertionEndTime = getVTime();
                        mode = NodeMode.INSERTED;
                    }
                    joinFuture.complete(rc);
                    return false;
                });
            }
            return false;
        });
        post(ev);
    }

    public CompletableFuture<Boolean> leaveAsync() throws IllegalStateException {
        System.out.println("Node " + this + " leaves");
        if (mode != NodeMode.INSERTED) {
           throw new IllegalStateException("not inserted");
        }
        mode = NodeMode.DELETING;
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        topStrategy.leave(future);
        return future;
    }

    public void fail() {
        System.out.println("*** " + this + " fails");
        this.isFailed = true;
    }
    
    public void revive() {
        System.out.println("*** " + this + " revives");
        this.isFailed = false;
    }

    public boolean isFailed() {
        return this.isFailed;
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
        CompletableFuture<LookupDone> future = new CompletableFuture<>();
        Event ev = new Lookup(this, key, this, future);
        future.handle((done, exc) -> {
            if (exc != null) {
                System.out.println("Lookup failed: " + exc);
                return false;
            }
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
            return false;
        });
        this.post(ev);
    }

    public Node getClosestPredecessor(DdllKey k) {
        Comparator<Node> comp = getComparator(k);
        List<Node> nodes = topStrategy.getAllLinks2();
        //Collections.sort(nodes, comp);
        //System.out.println("nodes = " + nodes);
        Optional<Node> n = nodes.stream().max(comp);
        //System.out.println("key = " + key);
        //System.out.println("max = " + n);
        return n.orElse(null);
    }

    private static Comparator<Node> getComparator(DdllKey k) {
        Comparator<Node> comp = (Node a, Node b) -> {
            // aの方がkに近ければ正の数，bの方がkeyに近ければ負の数を返す
            // [a, key, b) -> plus
            // [b, key, a) -> minus
            if (Node.isOrdered(a.key, true, k, b.key, false)) {
                return +1;
            }
            return -1;
        };
        return comp;
    }

    /**
     * 経路表から，範囲 [myKey, k) にキーが含まれるノードを抽出し，kから見てsuccessor
     * 方向にソートして返す．ただし，myKey は最初のノードとする．
     * 
     * <p>myKey = 100, k = 200 の場合，returnするリストは例えば [100, 150]． 
     * <p>myKey = k = 100 の場合 [100, 200, 300, 0]．
     * 
     * @param k
     * @return
     */
    public List<Node> getNodesForFix(DdllKey k) {
        Comparator<Node> comp = getComparator(k);
        List<Node> nodes = topStrategy.getAllLinks2();
        List<Node> cands = nodes.stream()
                .filter(p -> Node.isOrdered(this.key, true, p.key, k, false))
                .sorted(comp)
                .distinct()
                .collect(Collectors.toCollection(ArrayList::new));
        if (cands.get(cands.size() - 1) == this) {
            cands.remove(cands.size() - 1);
            cands.add(0, this);
        }
        return cands;
    }
    
    /*public static void main(String args[]) {
        DdllKey k0 = new DdllKey(0, new UniqId("0"));
        DdllKey k1 = new DdllKey(1, new UniqId("1"));
        DdllKey k2 = new DdllKey(2, new UniqId("2"));
        DdllKey k3 = new DdllKey(3, new UniqId("3"));
        Node n0 = new Node(k0, null, 0);
        Node n1 = new Node(k1, null, 0);
        Node n2 = new Node(k2, null, 0);
        Comparator<Node> comp = LocalNode.getComparator(k1);
        Node[] nodes = new Node[]{n0, n1, n2};
        List<Node> x = Arrays.asList(nodes).stream()
                .sorted(comp).collect(Collectors.toList());
        System.out.println(x);
    }*/
}
