package org.piax.gtrans.async;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.subspace.Range;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.async.Event.LocalEvent;
import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.EventException.RPCEventException;
import org.piax.gtrans.async.EventException.RetriableException;
import org.piax.gtrans.async.EventException.TimeoutException;
import org.piax.gtrans.async.EventSender.EventSenderNet;
import org.piax.gtrans.async.EventSender.EventSenderSim;
import org.piax.gtrans.ov.async.rq.RQAdapter;
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

    // to support multi-keys
    private static Map<PeerId, SortedSet<LocalNode>> localNodeMap
        = new ConcurrentHashMap<>();

    // stackable strategies
    ArrayList<NodeStrategy> strategies = new ArrayList<>();
    Map<Class<? extends NodeStrategy>, NodeStrategy> strategyMap = new HashMap<>();

    private LinkChangeEventCallback predChange;
    private LinkChangeEventCallback succChange;

    // incomplete requests 
    Map<Integer, RequestEvent<?, ?>> ongoingRequests = new HashMap<>();
    // requests that are not ack'ed
    Map<Integer, RequestEvent<?, ?>> unAckedRequests = new HashMap<>();
    
    // maybe-failed nodes
    // TODO: purge entries by timer!
    // TODO: define accessors!
    public Set<Node> maybeFailedNodes = new HashSet<>();

//    public static LocalNode newLocalNode(TransportId transId,
//            ChannelTransport<?> trans, Comparable<?> rawkey,
//            NodeStrategy strategy) 
//            throws IdConflictException, IOException {
//        DdllKey ddllkey = new DdllKey(rawkey, new UniqId(trans.getPeerId()));
//        LocalNode node = new LocalNode(transId, trans, ddllkey);
//        node.pushStrategy(strategy);
//        return node;
//    }

    @Deprecated
    public LocalNode(TransportId transId, ChannelTransport<?> trans,
            DdllKey ddllkey)
            throws IdConflictException, IOException {
        super(ddllkey, trans == null ? null : trans.getEndpoint());
        assert getInstance(ddllkey) == this; 
        assert trans == null || this.peerId.equals(trans.getPeerId());

        // to support multi-keys
        localNodeMap.computeIfAbsent(peerId, k -> new TreeSet<>())
            .add(this);

        if (trans == null) {
            this.sender = EventSenderSim.getInstance();
        } else {
            try {
                this.sender = new EventSenderNet(transId, trans);
            } catch (IdConflictException | IOException e) {
                throw e;
            }
        }
    }
    
    public LocalNode(EventSender sender, DdllKey ddllkey) {
        super(ddllkey, sender.getEndpoint());
        assert getInstance(ddllkey) == this; 
        // to support multi-keys
        localNodeMap.computeIfAbsent(peerId, k -> new TreeSet<>())
            .add(this);
        this.sender = sender;
    }

    /*
     * strategy
     */
    public void pushStrategy(NodeStrategy s) {
        strategies.add(s);
        strategyMap.put(s.getClass(), s);
        int i = strategies.size() - 1;
        s.level = i;
        s.activate(this);
    }

    public NodeStrategy getStrategy(Class<? extends NodeStrategy> clazz) {
        return strategyMap.get(clazz);
    }

    public NodeStrategy getUpperStrategy(NodeStrategy s) {
        if (s.level < strategies.size() - 1) {
            return strategies.get(s.level + 1);
        }
        return null;
    }

    public NodeStrategy getLowerStrategy(NodeStrategy s) {
        if (s.level > 0) {
            return strategies.get(s.level - 1);
        }
        return null;
    }

    public NodeStrategy getTopStrategy() {
        return strategies.get(strategies.size() - 1);
    }

    public NodeStrategy getBaseStrategy() {
        return strategies.get(0);
    }

    /**
     * replace this instance with corresponding Node object on serialization.
     * 
     * @return
     * @throws ObjectStreamException
     */
    private Object writeReplace() {
        Node repl = new Node(this.key, this.addr);
        return repl;
    }

    public void setLinkChangeEventHandler(LinkChangeEventCallback predChange,
            LinkChangeEventCallback succChange) {
        this.predChange = predChange;
        this.succChange = succChange;
    }

    @Override
    public String toString() {
        return super.toString() + (isFailed ? "(failed)" : ""); 
    }

    @Override
    public String toStringDetail() {
        return this.getTopStrategy().toStringDetail();
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

    public List<LocalNode> getSiblings() {
        return localNodeMap.get(peerId).stream()
                .filter(v -> v.isInserted())
                .collect(Collectors.toList());
    }
    
    public boolean isInserted() {
        // GRACE状態のノードもルーティング可能とするために，GRACEも挿入状態とみなす．
        return mode == NodeMode.INSERTED || mode == NodeMode.DELETING
                || mode == NodeMode.GRACE;
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
            failure = exc -> {
                Log.verbose(() -> "post: got exception: " + exc + ", " + ev);
                req.future.completeExceptionally(exc);
            };
        }
        ev.sender = ev.origin = this;
        ev.route.add(this);
        if (ev.routeWithFailed.size() == 0) {
            ev.routeWithFailed.add(this);
        }
        if (ev.delay == Node.NETWORK_LATENCY) {
            ev.delay = EventExecutor.latency(this, ev.receiver);
        }
        ev.failureCallback = failure;
        ev.vtime = EventExecutor.getVTime() + ev.delay;
        if (Log.verbose) {
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
                Log.verbose(() -> this + " got exception: " + e);
                if (failure != null) {
                    failure.run(new RPCEventException(e));
                }
            }
        }
    }

    /**
     * forward the event to the specified node.
     * 
     * @param dest  destination node
     * @param ev    event
     */
    public void forward(Node dest, Event ev) {
        this.forward(dest, ev, null);
    }

    public void forward(Node dest, Event ev, FailureCallback failure) {
        if (failure == null) {
            failure = exc -> {
                Log.verbose(() -> "forward: got exception: " + exc + ", " + ev);
            };
        }
        assert ev.origin != null;
        ev.beforeForwardHook(this);
        ev.sender = this;
        ev.failureCallback = failure;
        if (ev.delay == Node.NETWORK_LATENCY) {
            ev.delay = EventExecutor.latency(this, dest);
        }
        ev.receiver = dest;
        if (Log.verbose) {
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
                Log.verbose(()-> this + " got exception: " + e);
                failure.run(new RPCEventException(e));
            }
        }
    }

    private long getVTime() {
        return EventExecutor.getVTime();
    }

    public long getInsertionTime() {
        if (insertionEndTime == 0) {
            throw new Error("not inserted");
        }
        return insertionEndTime - insertionStartTime;
    }

    public int getMessages4Join() {
        return getTopStrategy().getMessages4Join();
    }

    public void addMaybeFailedNode(Node node) {
        maybeFailedNodes.add(node);
        getTopStrategy().foundMaybeFailedNode(node);
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
        getTopStrategy().initInitialNode();
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
        Consumer<Throwable> retry = (exc) -> {
            Log.verbose(() -> this + ": joinAsync failed: " + exc
                    + ", count=" + count);
            // reset insertionStartTime ?
            if (((exc instanceof RetriableException) 
                    || (exc instanceof TimeoutException))
                    && count > 1) {
                joinAsync(introducer, count - 1, joinFuture);
            } else {
                joinFuture.completeExceptionally(exc);
            }
        };
        Lookup ev = new Lookup(introducer, key);
        ev.onReply((results, exc) -> {
            if (exc != null) {
                retry.accept(exc);
            } else {
                if (results.succ == null) {
                    // Lookup failure
                    assert results.pred == null;
                    retry.accept(new TimeoutException());
                    return;
                }
                CompletableFuture<Boolean> future = new CompletableFuture<>();
                getTopStrategy().join(results, future);
                future.whenComplete((rc, exc2) -> {
                    if (exc2 != null) {
                        mode = NodeMode.OUT;
                        retry.accept(exc2);
                        return;
                    }
                    if (rc) {
                        insertionEndTime = getVTime();
                        mode = NodeMode.INSERTED;
                    }
                    joinFuture.complete(rc);
                });
            }
        });
        post(ev);
    }

    public CompletableFuture<Boolean> leaveAsync() {
        System.out.println("Node " + this + " leaves");
        if (mode != NodeMode.INSERTED) {
            CompletableFuture<Boolean> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("not inserted"));
            return f;
        }
        mode = NodeMode.DELETING;
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        LocalEvent ev = new LocalEvent(this, () -> {
            getTopStrategy().leave(future);
        });
        post(ev);
        CompletableFuture<Boolean> f = future.thenApply(rc -> {
            if (rc) {
                cleanup();
            }
            return rc;
        });
        return f;
    }
    
    public <T> void rangeQueryAsync(Collection<? extends Range<?>> ranges,
            RQAdapter<T> adapter, TransOptions opts) {
        getTopStrategy().rangeQuery(ranges, adapter, opts);
    }

    public <T> void forwardQueryLeftAsync(Range<?> range, int num,
            RQAdapter<T> adapter, TransOptions opts) {
        getTopStrategy().forwardQueryLeft(range, num, adapter, opts);
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
     * returns a stream of "active" nodes, that are nodes suitable for routing.
     * @return stream of active nodes
     */
    public Stream<Node> getActiveNodeStream() {
        List<FTEntry> allNodes = getTopStrategy().getRoutingEntries();

        // collect [me, successor)
        List<Node> successors = new ArrayList<>();
        for (LocalNode v : getSiblings()) {
            successors.add(v.succ);
        }
        // XXX: merge maybeFailedNode over siblings
        return allNodes.stream()
                .map(ent -> ent.allNodes())
                .flatMap(list -> {
                    Optional<Node> p = list.stream()
                            .filter(node -> 
                                (successors.contains(node)
                                || !maybeFailedNodes.contains(node))) 
                            .findFirst();
                    return streamopt(p);
                })
                .distinct();
    }

    private static <T> Stream<T> streamopt(Optional<T> opt) {
        if (opt.isPresent()) {
            return Stream.of(opt.get());
        } else {
            return Stream.empty();
        }
    }

    public Node getClosestPredecessor(DdllKey k) {
        Comparator<Node> comp = getComparator(k);
        Optional<Node> n = getActiveNodeStream().max(comp);
        return n.orElse(null);
    }

    public static Comparator<Node> getComparator(DdllKey k) {
        Comparator<Node> comp = (Node a, Node b) -> {
            assert a != b;
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
        List<FTEntry> all = getTopStrategy().getRoutingEntries();
        List<Node> cands = all.stream()
            .map(ent -> ent.allNodes())
            .flatMap(list -> list.stream())
            .filter(p -> Node.isOrdered(this.key, true, p.key, k, false))
            .distinct()
            .sorted(comp)
            .collect(Collectors.toCollection(ArrayList::new));
        if (cands.size() > 0 && cands.get(cands.size() - 1) == this) {
            cands.remove(cands.size() - 1);
            cands.add(0, this);
        }
        return cands;
    }

    public void cleanup() {
        localNodeMap.get(peerId).remove(this);
    }
}
