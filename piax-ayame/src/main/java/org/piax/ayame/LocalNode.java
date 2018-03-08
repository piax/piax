package org.piax.ayame;

import java.io.IOException;
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

import org.piax.ayame.Event.ErrorEvent;
import org.piax.ayame.Event.Lookup;
import org.piax.ayame.Event.RequestEvent;
import org.piax.ayame.EventException.GraceStateException;
import org.piax.ayame.EventException.NetEventException;
import org.piax.ayame.EventException.RetriableException;
import org.piax.ayame.EventException.TimeoutException;
import org.piax.ayame.EventSender.EventSenderSim;
import org.piax.ayame.Node.NodeMode;
import org.piax.ayame.ov.rq.RQAdapter;
import org.piax.common.DdllKey;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.subspace.Range;
import org.piax.gtrans.TransOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalNode extends Node {
    private static final Logger logger = LoggerFactory.getLogger(LocalNode.class);
    public static final int INSERTION_DELETION_RETRY = 10; 
    public long insertionStartTime = -1L;
    public long insertionEndTime;

    protected EventSender sender; // XXX used to be a final field
    public Node introducer;
    public Node succ, pred;
    public NodeMode mode = NodeMode.OUT;
    private boolean isFailed = false;   // for simulation

    // for statistics
    public Counters counters = new Counters();

    // to support multi-keys
    protected static Map<PeerId, SortedSet<LocalNode>> localNodeMap
        = new ConcurrentHashMap<>();

    // stackable strategies
    ArrayList<NodeStrategy> strategies = new ArrayList<>();
    Map<Class<? extends NodeStrategy>, NodeStrategy> strategyMap = new HashMap<>();

    protected transient List<Runnable> cleanup = new ArrayList<>();

    private LinkChangeEventCallback predChange;
    private LinkChangeEventCallback succChange;

    // incomplete requests 
    Map<Integer, RequestEvent<?, ?>> ongoingRequests = new HashMap<>();
    // requests that are not ack'ed
    Map<Integer, RequestEvent<?, ?>> unAckedRequests = new HashMap<>();

    public final static int PURGE_FAILED_NODE_TIME = 2*60*1000; 
    private Set<Node> possiblyFailedNodes = new HashSet<>();

    public LocalNode(DdllKey ddllkey, Endpoint e) {
        this(EventSenderSim.getInstance(), ddllkey);
    }

    public LocalNode(EventSender sender, DdllKey ddllkey) {
        super(ddllkey, sender.getEndpoint());
        assert getInstance(ddllkey) == this; 
        // to support multi-keys
        localNodeMap.computeIfAbsent(peerId, k -> new TreeSet<>())
            .add(this);
        cleanup.add(() -> localNodeMap.get(peerId).remove(this));
        cleanup.add(() -> possiblyFailedNodes.clear());
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

    void receive(Event ev) {
        addToRoute(ev.routeWithFailed);
        if ((isFailed() || this.mode == NodeMode.DELETED)) {
            logger.trace("message received by not inserted or failed node: {}",
                    ev);
            return;
        }
        if (this.mode == NodeMode.GRACE) {
            logger.debug("{}: received in grace period: {}", this, ev);
            if (ev instanceof RequestEvent && !(ev instanceof Lookup)) {
                post(new ErrorEvent((RequestEvent<?, ?>)ev, 
                        new GraceStateException()));
            }
            return;
        }
        addToRoute(ev.route);
        if (ev.beforeRunHook(this)) {
            ev.run();
        }
    }

    private void addToRoute(List<Node> route) {
        if (route.isEmpty() || route.get(route.size() - 1) != this) {
            route.add(this);
        }
    }

    /**
     * post a event
     *
     * @param ev an event to post
     */
    public void post(Event ev) {
        post(ev, null);
    }

    public void post(Event ev, FailureCallback failure) {
        if (ev instanceof RequestEvent && failure == null) {
            RequestEvent<?, ?> req = (RequestEvent<?, ?>)ev;
            failure = exc -> {
                logger.trace("post: got exception: {}. {}", exc, ev);
                req.cleanup();
                req.future.completeExceptionally(exc);
            };
        }
        ev.sender = ev.origin = this;
        ev.failureCallback = failure;
        ev.route.add(this);
        if (ev.routeWithFailed.size() == 0) {
            ev.routeWithFailed.add(this);
        }
        ev.beforeSendHook(this);
        if (!isFailed) {
            sender.send(ev).whenComplete((result, e) ->{
                if (e != null && ev.failureCallback != null) {
                    // It might be completed on the receiver transport thread.
                    // Ensure to run on the execution thread.
                    EventExecutor.runNow("post: failureCallback", () -> {
                        ev.failureCallback.run(new NetEventException(e));
                    });
                }
            });
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
        assert ev.origin != null;
        if (failure == null) {
            failure = exc -> {
            	logger.trace("forward: got exception: {}, {}", exc, ev);
            };
        }
        ev.failureCallback = failure;
        ev.beforeForwardHook(this);
        ev.sender = this;
        ev.receiver = dest;
//        if (ev.delay == Node.NETWORK_LATENCY) {
//            ev.delay = EventExecutor.latency(this, dest);
//        }
//        if (Log.verbose) {
//            if (ev.delay != 0) {
//                System.out.println(this + "|forward to " + dest + ", " + ev
//                        + ", (arrive at T" + ev.vtime + ")");
//            } else {
//                System.out.println(this + "|forward to " + dest + ", " + ev);
//            }
//        }
        if (!isFailed()) {
            sender.send(ev).whenComplete((result, e) ->{
                if (e != null && ev.failureCallback != null) {
                    // It might be completed on the receiver transport thread.
                    // Ensure to run on the execution thread.
                    EventExecutor.runNow("forward: failureCallback", () -> {
                        ev.failureCallback.run(new NetEventException(e));
                    });
                }
            });
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

    public void addPossiblyFailedNode(Node node) {
        logger.trace("{}: addPossiblyFailedNode: {}", this, node);
        if (node != this) {
            if (!possiblyFailedNodes.contains(node)) {
                possiblyFailedNodes.add(node);
                // schedule a purge event.
                // the purge event is cancelled on cleanup but this cleanup
                // is also cancelled on purge.... 
                Indirect<Runnable> cancel = new Indirect<>();
                Event purge = EventExecutor.sched("purge-failed-node",
                        PURGE_FAILED_NODE_TIME, () -> {
                            possiblyFailedNodes.remove(node);
                            cleanup.remove(cancel.val);
                        });
                cancel.val = () -> EventExecutor.cancelEvent(purge);
                cleanup.add(cancel.val);
            }
        }
    }

    public boolean isPossiblyFailed(Node node) {
        return possiblyFailedNodes.contains(node);
    }

    /**
     * insert a key into a ring.
     * 
     * @param introducer a node that has been inserted to the ring.
     * @throws IOException thrown in communication errors
     * @throws InterruptedException thrown if interrupted
     */
    public void addKey(Endpoint introducer) throws IOException,
        InterruptedException {
        logger.debug("{}: addkey", this);
        Node temp = Node.getWildcardInstance(introducer);
        CompletableFuture<Void> future = joinAsync(temp);
        try {
            future.get();
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        } catch (InterruptedException e) {
            throw e;
        }
    }

    public CompletableFuture<Void> addKeyAsync(Endpoint introducer) {
        logger.debug("{}: addkeyAsync", this);
        Node temp = Node.getWildcardInstance(introducer);
        return joinAsync(temp);
    }

    public void removeKey() throws IOException, InterruptedException {
        logger.debug("{}: removeKey", this);
        CompletableFuture<Void> future = leaveAsync();
        try {
            future.get();
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
     *
     * @param introducer any node that has been inserted in the network
     * @return CompletableFuture that completes on insertion
     *         success or failure.  The result can be obtained with the
     *         boolean value.
     */
    public CompletableFuture<Void> joinAsync(Node introducer) { 
        CompletableFuture<Void> joinFuture = new CompletableFuture<>();
        EventExecutor.runNow("joinAsync", () -> {
            joinAsync(introducer, INSERTION_DELETION_RETRY, joinFuture);
        });
        return joinFuture;
    }

    /**
     * locate the node position and insert
     * @param introducer
     * @param count      number of remaining retries
     * @param joinFuture
     */
    private void joinAsync(Node introducer, int count,
            CompletableFuture<Void> joinFuture) {
        if (insertionStartTime == -1) {
            insertionStartTime = getVTime();
        }
        this.mode = NodeMode.INSERTING;
        this.introducer = introducer;
        Consumer<Throwable> retry = (exc) -> {
            logger.trace("{}: joinAsync failed: {}, count={}", this, exc, count);
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
                CompletableFuture<Void> future = new CompletableFuture<>();
                getTopStrategy().join(results, future);
                future.whenComplete((rc, exc2) -> {
                    if (exc2 != null) {
                        mode = NodeMode.OUT;
                        retry.accept(exc2);
                        return;
                    }
                    insertionEndTime = getVTime();
                    mode = NodeMode.INSERTED;
                    joinFuture.complete(null);
                });
            }
        });
        post(ev);
    }

    public CompletableFuture<Void> leaveAsync() {
        logger.debug("Node {} leaves", this);
        if (mode != NodeMode.INSERTED) {
            CompletableFuture<Void> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("not inserted"));
            return f;
        }
        mode = NodeMode.DELETING;
        CompletableFuture<Void> future = new CompletableFuture<>();
        EventExecutor.runNow("leaveAsync", () -> {
            getTopStrategy().leave(future);
        });
        return future.thenRun(() -> {
            cleanup();
        });
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
        logger.debug("*** {} fails", this);
        this.isFailed = true;
    }
    
    public void revive() {
        logger.debug("*** {} revives", this);
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
        // XXX: merge possiblyFailedNodes over siblings
        return allNodes.stream()
                .map(ent -> ent.allNodes())
                .flatMap(list -> {
                    Optional<Node> p = list.stream()
                            .filter(node -> 
                                (successors.contains(node)
                                || !possiblyFailedNodes.contains(node))) 
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
     * @param k key of upper limit 
     * @return List of Node
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
        cleanup.stream().forEach(r -> r.run());
        cleanup.clear();
    }

    // called from EventExecutor.reset()
    public static void resetLocalNodeMap() {
        localNodeMap.clear();
    }
}
