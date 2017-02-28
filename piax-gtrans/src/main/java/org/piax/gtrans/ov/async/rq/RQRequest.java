package org.piax.gtrans.ov.async.rq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.piax.common.Id;
import org.piax.common.PeerId;
import org.piax.common.subspace.CircularRange;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.ReturnValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.StreamingRequestEvent;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.NetworkParams;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;
import org.piax.gtrans.ov.ring.rq.RQManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a class used for range queries.
 * <p>
 * this class contains various data that are required to be transmitted to the
 * target nodes. this class also contains {@link #failedLinks} field, which
 * represents a set of failed nodes that are found while processing the range
 * query.
 * <p>
 * this class also manages (partial) results returned from child nodes.
 * 
 */
public class RQRequest<T> extends StreamingRequestEvent<RQRequest<T>, RQReply<T>> {
    /*--- logger ---*/
    private static final Logger logger =
            LoggerFactory.getLogger(RQRequest.class);
    private static final long serialVersionUID = 1L;

    final long qid;
    Node root;       // DIRECT only
    int rootEventId; // DIRECT only
    protected final Collection<RQRange> targetRanges;
    final RQValueProvider<T> provider;
    final TransOptions opts;
    final boolean isRoot;
    transient Consumer<RemoteValue<T>> resultsReceiver;  // root only

    /**
     * failed links. this field is used for avoiding and repairing dead links.
     */
    final Set<Node> obstacles;

    transient RQCatcher catcher; // receiver half only

    enum SPECIAL {
        PADDING;
    }

    /**
     * create a root RQRequest.
     * 
     * @param ranges set of query ranges
     * @param query an object sent to all the nodes within the query ranges
     * @param opts the transport options.
     * @param resultReceiver
     */
    public RQRequest(Node receiver, Collection<RQRange> ranges, 
            RQValueProvider<T> provider, TransOptions opts, 
            Consumer<RemoteValue<T>> resultReceiver) {
        this(receiver, true, ranges, provider, opts, resultReceiver, null);
    }

    /*
     * isRoot -> isReceiverHalf
     * isRoot <-> resultReceiver != null
     * !isReceiverHalf -> resultReceiver == null
     */
    private RQRequest(Node receiver, 
            boolean isReceiverHalf, Collection<RQRange> ranges,
            RQValueProvider<T> provider, TransOptions opts,
            Consumer<RemoteValue<T>> resultsReceiver,
            RQRequest<T> parent) {
        super(receiver, isReceiverHalf);
        this.isRoot = resultsReceiver != null;
        assert !isRoot || isReceiverHalf; // isRoot -> isReceiverHalf
        assert isReceiverHalf || !isRoot; // !isReceiverHalf -> !isRoot

        super.setReplyReceiver((RQReply<T> rep) -> {
            parent.catcher.replyReceived(rep);
        });
        super.setExceptionReceiver((Throwable exc) -> {
            handleErrors(exc);
        });

        this.qid = EventExecutor.random().nextLong();
        this.root = null;  // overridden by DirectResponder at root node
        this.rootEventId = 0; // overridden by DirectResponder at root node
        this.resultsReceiver = resultsReceiver;
        this.targetRanges = Collections.unmodifiableCollection(ranges);
        this.provider = provider;
        this.opts = opts;
        this.obstacles = new HashSet<>();
        if (isReceiverHalf) {
            this.catcher = new RQCatcher();
        }
    }

    /**
     * create a child instance of specified RQRequest.
     * 
     * @param parent
     * @param receiver
     * @param newRanges
     */
    private RQRequest(RQRequest<T> parent, Node receiver,
            Collection<RQRange> newRanges) {
        super(receiver, false);
        this.isRoot = false;
        super.setReplyReceiver((RQReply<T> rep) -> {
            parent.catcher.replyReceived(rep);
        });
        super.setExceptionReceiver((Throwable exc) -> {
            handleErrors(exc);
        });
        this.qid = parent.qid;
        this.root = parent.root;
        this.rootEventId = parent.rootEventId;
        this.resultsReceiver = parent.resultsReceiver;
        this.targetRanges = newRanges;
        this.provider = parent.provider;
        this.opts = parent.opts;
        this.obstacles = parent.obstacles;
    }

    /**
     * create a child RQRequest from this instance.
     * <p>
     * this method is used both at intermediate nodes and at root node (in slow
     * retransmission case)
     * 
     * @param newRQRange new RQRange for the child RQMessage
     * @return a instance of child RQMessage
     */
    private RQRequest<T> newChildInstance(Node receiver,
            Collection<RQRange> newRQRange) {
        return new RQRequest<>(this, receiver, newRQRange);
    }

    @Override
    public String toStringMessage() {
        return "RQRequest["
                + "qid=" + qid
                + ", opts=" + opts
                + ", isRoot=" + isRoot
                + ", sender=" + sender
                + ", root=" + root
                + ", rootEvId=" + rootEventId
                + ", target=" + targetRanges
                + ", obstacles=" + obstacles + "]";
    }

    @Override
    protected long getAckTimeoutValue() {
        if (opts.getResponseType() == ResponseType.NO_RESPONSE) {
            return 0;
        } else {
            return NetworkParams.NETWORK_TIMEOUT;
        }
    }

    @Override
    protected long getReplyTimeoutValue() {
        if (opts.getResponseType() == ResponseType.NO_RESPONSE) {
            return 0;
        } else {
            // XXX: 再送時には短いタイムアウトで!
            return opts.getTimeout();
        }
    }

    public void addObstacles(Collection<Node> links) {
        obstacles.addAll(links);
    }

    public Collection<RQRange> getTargetRanges() {
        return targetRanges;
    }

    @Override
    public void run() {
        if (catcher == null) {
            this.catcher = new RQCatcher();
        }
        catcher.rqDisseminate();
    }
 
    public void receiveReply(RQReplyDirect<T> rep) {
        logger.debug("RQRequest: direct reply received: {}", rep);
        catcher.replyReceived(rep);
    }

    private void handleErrors(Throwable exc) {
        logger.debug("RQRequest: got exception: {}, {}", this, exc.toString());
    }
    
    @Override
    protected Event clone() {
        RQRequest<?> ev = (RQRequest<?>)super.clone();
        ev.resultsReceiver = null;
        ev.catcher = null;
        return ev;
    }
    
    private static <T> Stream<T> streamopt(Optional<T> opt) {
        if (opt.isPresent()) {
            return Stream.of(opt.get());
        } else {
            return Stream.empty();
        }
    }

    /**
     * A class for storing results of a range query used by parent nodes.
     */
    class RQCatcher {
        
        /*
         * range [--------------------) 
         * rvals    [-)  [-)    [-)
         * gaps  [--) [--) [---)  [---)
         */

        /** return values */
        // XXX: return value 1つづつに DdllKeyRange を保持するのはメモリ効率が悪い．
        // XXX: 中継ノードから親ノードに渡すときに，連続するKeyRangeをまとめると良い．
        final NavigableMap<DdllKey, DKRangeRValue<T>> rvals;

        /** messages sent to children */
        final Set<RQRequest<T>> childMsgs = new HashSet<>();

        /** subranges that have not yet received any return values */
        final Set<RQRange> gaps;

        int retransCount = 0;

        final RQStrategy strategy;
        final Responder responder;
        final RQResults<T> results;

        public RQCatcher() {
            this.rvals = new ConcurrentSkipListMap<>();
            this.gaps = new HashSet<>(targetRanges);
            if (isRoot) {
                results = new RQResults<T>(this);
            } else {
                results = null;
            }
            this.strategy = RQStrategy.getRQStrategy(getLocalNode());

            switch (opts.getResponseType()) {
            case NO_RESPONSE:
                responder = new NoResponder();
                break;
            case DIRECT:
                responder = new DirectResponder();
                break;
            case AGGREGATE:
                responder = new AggregateResponder();
                break;
            default:
                throw new Error("unknown response type: " + opts.getResponseType());
            }
        }

        @Override
        public String toString() {
            return "RQCatcher[ID=" + getEventId()
            + ", rvals="
            + (rvals.size() > 10 ? "(" + rvals.size() + " entries)"
                    : rvals.values())
            + ", gaps=" + gaps + ", childMsgs="
            + childMsgs
            + ", retrans=" + retransCount + "]";
        }

        // XXX: note that wrap-around query ranges such as [10, 5) do not work
        // for now (k-abe)
        private void rqDisseminate() {
            logger.debug("rqDisseminate start: {}", this);
            logger.debug("                   : {}", RQRequest.this);
            Map<Id, List<RQRange>> map = assignDelegates();
            logger.debug("aggregated: {}", map);
            PeerId peerId = getLocalNode().getPeerId();

            /*
             * send aggregated requests to children.
             */
            map.entrySet().stream()
                .filter(ent -> !ent.getKey().equals(peerId))
                .forEach(ent -> {
                    List<RQRange> sub = ent.getValue();
                    Node dlg = sub.get(0).getNode();
                    RQRequest<T> m = newChildInstance(dlg, sub);
                    logger.debug("send to child: {}", m);
                    this.childMsgs.add(m);
                    getLocalNode().post(m);
                    cleanup.add(() -> m.cleanup());
                });

            // obtain values for local ranges
            CompletableFuture<List<DKRangeRValue<T>>> future
                = rqExecuteLocal(map.get(peerId));
            future.thenAccept((List<DKRangeRValue<T>> rvals) -> {
                addRemoteValues(rvals);
            });
            responder.rqDisseminateFinish();
            logger.debug("rqDisseminate finished");
        }

        /**
         * gapの各範囲を部分範囲に分割し，それぞれ担当ノードを割り当てる．
         * 各ノード毎に割り当てたRQRangeのリストのMapを返す．
         * 
         * @return a map of id and RQRanges
         */
        protected Map<Id, List<RQRange>> assignDelegates() {
            LocalNode local = getLocalNode(); 
            List<List<Node>> allNodes = strategy.getRoutingEntries();

            // collect [me, successor)
            List<Node> successors = new ArrayList<>();
            List<RQRange> succRanges = new ArrayList<>();
            for (LocalNode v : local.getSiblings()) {
                successors.add(v.succ);
                succRanges.add(new RQRange(v, v.key, v.succ.key));
            }

            // XXX: merge maybeFailedNode over siblings
            Set<Node> maybeFailedNodes = local.maybeFailedNodes;
            List<Node> actives = allNodes.stream()
                    .flatMap(list -> {
                        Optional<Node> p = list.stream()
                                .filter(node -> 
                                    (successors.contains(node)
                                    || !maybeFailedNodes.contains(node))) 
                                .findFirst();
                        return streamopt(p);
                    })
                    .distinct()
                    .collect(Collectors.toList());

            logger.debug("allNodes={}, actives={}, maybeFailed={}", allNodes,
                    actives, maybeFailedNodes);
            return gaps.stream()
                    .flatMap(range -> assignDelegate(range, actives, succRanges)
                            .stream())
                    .collect(Collectors.groupingBy((RQRange range)
                            -> range.getNode().key.getUniqId()
                            ));
        }

        /**
         * split the query range and assign a delegate node to each subrange.
         *  
         * @param queryRange    query range
         * @param actives
         * @param succRanges
         * @return list of RQRange
         */
        private List<RQRange> assignDelegate(RQRange queryRange,
                List<Node> actives, List<RQRange> succRanges) {
            List<RQRange> list = new ArrayList<>();

            // QR から [自ノード, successor) を削除し，自ノードを担当ノードとする
            //      [======QR======]
            //   [-----)
            //  me   succ
            for (RQRange range : succRanges) {
                DdllKey me = range.from;
                DdllKey succ = range.to;
                if (!queryRange.contains(me) && queryRange.contains(succ)
                        && queryRange.from.compareTo(succ) != 0) {
                    RQRange sub = new RQRange(range.getNode(),
                            queryRange.from, succ);
                    sub.assignSubId(queryRange);
                    list.add(sub);
                    queryRange = new RQRange(null, succ, queryRange.to,
                            queryRange.ids);
                }
            }
            logger.debug("succRanges={}, list={}", succRanges, list);

            RQRange qr = queryRange;  // make effectively final
            // included = nodes that are contained in the queryRange
            List<Node> included = actives.stream()
                    .filter(node -> qr.contains(node.key))
                    .sorted()
                    .collect(Collectors.toList());
            logger.debug("included={}", included);

            // 左端の担当ノードを決める
            Node first = included.isEmpty() ? null : included.get(0);
            if (first == null || queryRange.from.compareTo(first.key) != 0) {
                // includedが空，もしくはincludedの最初のエントリがqueryRangeの途中
                Node d = strategy.getClosestPredecessor(queryRange.from,
                        actives);
                RQRange r = new RQRange(d, queryRange.from, 
                        (first == null ? queryRange.to : first.key),
                        queryRange.ids);
                list.add(r);
            }

            for (int i = 0; i < included.size(); i++) {
                Node ent = included.get(i);
                Node next = (i == included.size() - 1) ? null : included.get(i + 1);
                DdllKey from = ent.key;
                DdllKey to = (next == null ? queryRange.to: next.key); 
                RQRange r = new RQRange(ent, from, to);
                r.assignSubId(queryRange);
                list.add(r);
            }
            logger.debug("assignDelegate: returns {}", list);
            return list;
        }

        /**
         * called when a reply message is received.
         * 
         * @param reply the reply message.
         */
        void replyReceived(RQReply<T> reply) {
            logger.debug("RQRequest: reply received: {}", reply);
            if (reply.isFinal) {
                reply.req.cleanup(); // cleanup sender half
                boolean rc = childMsgs.remove(reply.req);
                assert rc;
            }
            addRemoteValues(reply.vals);
            if (isCompleted()) {
                cleanup();
            }
        }

        void replyReceived(RQReplyDirect<T> reply) {
            addRemoteValues(reply.vals);
            if (isCompleted()) {
                cleanup();
            }
        }

        /**
         * store results of (sub)ranges and flush if appropriate.
         * 
         * @param ranges the ranges.
         * @returns true if flushed.
         */
        private void addRemoteValues(Collection<DKRangeRValue<T>> ranges) {
            if (ranges == null) {
                // ACKの代わりにRQReplyを受信した場合
                return;
            }
            for (DKRangeRValue<T> range : ranges) {
                addRemoteValue(range.getRemoteValue(), range);
            }
            logger.debug("gaps={}", gaps);
            responder.onReceiveValues();
//            if (isCompleted() || opts.getResponseType() == ResponseType.DIRECT) {
//                // transmit the collected results to the parent node
//                flush();
//                return true;
//            } else {
//                return false;
//                // fast flushing
//                // finished + failed = children ならば flush
//                /*Set<Endpoint> eset = new HashSet<>();
//                eset.addAll(children);
//                eset.removeAll(finished);
//                eset.removeAll(failedLinks);
//                logger.debug("children={}, finished={}, failed={}, eset={}",
//                        children, finished, failedLinks, eset);
//                if (eset.size() == 0) {
//                    flush();
//                }*/
//            }
        }

        private void addRemoteValue(RemoteValue<T> rval, Range<DdllKey> range) {
            if (rvals.containsKey(range.from)) {
                return;
            }

            // find the gap that contains range.from 
            RQRange gap = gaps.stream()
                .filter(e -> e.contains(range.from))
                .findAny()
                .orElse(null);
            if (gap == null) {
                logger.info("no gap instance: {} in {}", range.from, this);
                return;
            }
            // delete the range r from gaps
            gaps.remove(gap);
            List<CircularRange<DdllKey>> retains = gap.retain(range, null);
            // add the remaining ranges to gaps
            if (retains != null) {
                for (CircularRange<DdllKey> p : retains) {
                    RQRange s = new RQRange(gap.getNode(), p.from, p.to, gap.ids);
                    gaps.add(s);
                }
            }
            logger.debug("addRV: gap={}, r={}, retains={}, gaps={}", gap, range,
                    retains, gaps);
            rvals.put(range.from, new DKRangeRValue<>(rval, range));

            if (isRoot) {
                Object v = rval.getValue();
                if (v instanceof MVal) {
                    // MVal should be removed
                    @SuppressWarnings("unchecked")
                    MVal<T> mval = (MVal<T>)v;
                    for (ReturnValue<T> o : mval.vals) {
                        notifyResult(new RemoteValue<>(
                                rval.getPeer(), o.getValue()));
                    }
                } else {
                    notifyResult(rval);
                }
                if (isCompleted()) {
                    notifyResult(null);    // notify completeness by NULL
                }
            }
        }
        
        private void notifyResult(RemoteValue<T> rval) {
            if (resultsReceiver == null) {
                return;
            }
            if (rval == null || rval.getValue() != SPECIAL.PADDING) {
                resultsReceiver.accept(rval);
            }
        }

        public RQResults<?> getRQResults() {
            return results;
        }

        boolean isCompleted() {
            return gaps.isEmpty();
        }

        /**
         * obtain a result value from local node.
         *  
         * @param ranges このノードが担当する範囲のリスト．各範囲はtargetRangeに含まれる
         * @returns CompletableFuture
         */
        private CompletableFuture<List<DKRangeRValue<T>>>
        rqExecuteLocal(List<RQRange> ranges) {
            logger.debug("rqExecuteLocal: ranges={}", ranges);
            // results of locally-resolved ranges
            List<DKRangeRValue<T>> rvals = new ArrayList<>();
            if (ranges == null) {
                return CompletableFuture.completedFuture(rvals);
            }
            @SuppressWarnings("unchecked")
            CompletableFuture<Void> futures = ranges.stream()
                .map(r -> {
                    // 1) obtain a value from provider
                    CompletableFuture<T> f;
                    if (!r.contains(r.getNode().key)) {
                        // although SPECIAL.PADDING is not a type of T, using
                        // it as T is safe because it is used just as a marker. 
                        f = CompletableFuture.completedFuture((T)SPECIAL.PADDING);
                    } else {
                        // XXX: consider the case where provider throws exception
                        f = provider.getRaw((LocalNode)r.getNode(), qid, r.getNode().key);
                    }
                    return f.thenAccept((T val) -> {
                        // 2) on provider completion, adds the value to rvals 
                        RemoteValue<T> rval = new RemoteValue<>(getLocalNode().peerId, val);
                        rvals.add(new DKRangeRValue<T>(rval, r));
                    });
                })
                .reduce((a, b) -> a.runAfterBoth(b, () -> {}))
                .orElse(null);
            CompletableFuture<List<DKRangeRValue<T>>> rc = new CompletableFuture<>();
            futures.thenRun(() -> rc.complete(rvals));
            return rc;
        }

        /*
         * Message Sequences:
         * 
         * NO_RESPONSE:
         * 
         *   ROOT----RQRequest---->CHILD1                 CHILD2
         *     |                     |------RQRequest------>|
         *
         * 
         * DIRECT:
         *
         *   ROOT----RQRequest---->CHILD1                 CHILD2
         *     |<-----RQReply--------|------RQRequest------>|
         *     |                     |<----RQReply(dummy)---|
         *     |<-------------------------RQReplyDirect-----|
         *  
         *  - always send RQReply to the parent node.
         *  - no AckEvent is used. 
         * 
         * AGGREGATE:
         *
         *   ROOT----RQRequest---->CHILD1                 CHILD2
         *     |<-----AckEvent-------|------RQRequest------>|
         *     |                     |<------RQReply--------|
         *     |<-----RQReply--------|                      |
         *
         *  - AckEvents are sent only from intermediate nodes.
         */
        abstract class Responder {
            TimerEvent expirationTask;
            TimerEvent slowRetransTask;
            // called when rqDisseminate has finished
            abstract void rqDisseminateFinish();
            // called when some return values are available
            abstract void onReceiveValues();
            protected void startExpirationTask() {
                long expire = opts.getTimeout();
                if (expirationTask != null || expire == 0) {
                    return;
                }
                long exp = expire + (isRoot ? 0 : RQManager.RQ_EXPIRATION_GRACE);
                logger.debug("schedule expiration after {}", exp);
                expirationTask = EventExecutor.sched("expire-" + getEventId(),
                        exp, () -> {
                            logger.debug("expired: {}", this);
                            cleanup();
                            notifyResult(null);
                        });
                cleanup.add(() -> expirationTask.cancel());
            }
            protected void startSlowRetranstTask() {
                assert isRoot;
                assert slowRetransTask == null;
                RetransMode rmode = opts.getRetransMode();
                if (rmode == RetransMode.SLOW || rmode == RetransMode.RELIABLE) {
                    // schedule slow retransmission task
                    long retrans = RQManager.RQ_RETRANS_PERIOD;
                    logger.debug("schedule slow retransmission every {}", retrans);
                    slowRetransTask = EventExecutor.sched(
                            "slowretrans-" + getEventId(),
                            retrans, retrans, () -> {
                                logger.debug("start slow retransmit");
                                rqDisseminate();
                            });
                    cleanup.add(() -> {
                        slowRetransTask.cancel();
                    });
                }
            }
        }

        class NoResponder extends Responder {
            @Override
            void rqDisseminateFinish() {
                if (isRoot) {
                    notifyResult(null);
                }
            }
            @Override
            void onReceiveValues() {
                // empty
            }
        }

        class AggregateResponder extends Responder {
            TimerEvent flushTask;
            boolean flushed;
            public AggregateResponder() {
                if (isRoot) {
                    startSlowRetranstTask();
                }
            }
            @Override
            void rqDisseminateFinish() {
                if (!isRoot && !flushed) {
                    // if we have some value, flush it immediately instead of
                    // just sending an ack.
                    if (!rvals.isEmpty()) {
                        flush();
                    } else {
                        getLocalNode().post(new AckEvent(RQRequest.this, sender));
                    }
                }
                if (!isCompleted()) {
                    startExpirationTask();
                }
            }
            @Override
            void onReceiveValues() {
                if (!isRoot && isCompleted()) {
                    flush();
                    flushed = true;
                    if (flushTask != null) {
                        flushTask.cancel();
                    }
                } else if (!isRoot && flushTask == null) {
                    long flush = RQManager.RQ_FLUSH_PERIOD;
                    logger.debug("schedule periodic flushing {}", flushTask);
                    flushTask = EventExecutor.sched("flush-" + getEventId(), 
                            flush, flush, () -> flush());
                    cleanup.add(() -> {
                        flushTask.cancel();
                    });
                }
            }
            private void flush() {
                if (!rvals.isEmpty()) {
                    // if we don't copy, we'll send an empty list
                    Collection<DKRangeRValue<T>> copy = new ArrayList<>(rvals.values());
                    Event ev = new RQReply<T>(RQRequest.this, copy, isCompleted());
                    getLocalNode().post(ev);
                    rvals.clear();
                }
            }
        }
        
        class DirectResponder extends Responder {
            boolean acked;
            public DirectResponder() {
                if (isRoot) {
                    LocalNode local = (LocalNode)receiver;
                    RQRequest.this.root = local;
                    RQRequest.this.rootEventId = getEventId();
                    // this instance receives RQReplyDirect messages
                    RequestEvent.registerRequestEvent(local, RQRequest.this);
                    cleanup.add(() -> {
                        RequestEvent.removeRequestEvent(local, getEventId());
                    });
                    startSlowRetranstTask();
                }
            }
            @Override
            void rqDisseminateFinish() {
                if (!isRoot && !acked) {
                    // send empty RQReply to parent instead of Ack
                    Event ev = new RQReply<T>(RQRequest.this, null, true);
                    getLocalNode().post(ev);
                }
                if (isRoot && !isCompleted()) {
                    startExpirationTask();
                }
            }
            @Override
            void onReceiveValues() {
                if (!isRoot) {
                    flush();
                }
            }
            private void flush() {
                if (sender == root) {
                    Event ev = new RQReply<T>(RQRequest.this, rvals.values(), true);
                    getLocalNode().post(ev);
                    acked = true;
                } else {
                    if (rvals.size() > 0) {
                        Event ev = new RQReplyDirect<T>(RQRequest.this, rvals.values());
                        getLocalNode().post(ev);
                    }
                }
            }
        }
    }
    
    public static class MVal<T> implements Serializable {
        private static final long serialVersionUID = 1L;
        public List<ReturnValue<T>> vals =
                new ArrayList<ReturnValue<T>>();

        public String toString() {
            return "" + vals;
        }
    }
}