package org.piax.gtrans.ov.async.rq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.piax.gtrans.async.Node;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;
import org.piax.gtrans.ov.ring.rq.DdllKeyRange;
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

    Node root;       // DIRECT only
    int rootEventId; // DIRECT only

    /** the target ranges, that is not modified */
    protected final Collection<RQRange> targetRanges;
    final RQValueProvider<T> provider;
    final TransOptions opts;
    final boolean isRoot;
    transient Consumer<RemoteValue<T>> resultsReceiver;  // root only

    /**
     * failed links. this field is used for avoiding and repairing dead links.
     */
    final Set<Node> failedLinks;

    transient RQCatcher catcher; // parent only

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
     * isRoot -> isParent
     * isRoot <-> resultReceiver != null
     * !isParent -> resultReceiver == null
     */
    private RQRequest(Node receiver, 
            boolean isReceiverHalf, Collection<RQRange> ranges,
            RQValueProvider<T> provider, TransOptions opts,
            Consumer<RemoteValue<T>> resultsReceiver,
            RQRequest<T> parent) {
        super(receiver, isReceiverHalf);
        this.isRoot = resultsReceiver != null;
        assert !isRoot || isReceiverHalf; // isRoot -> isParent
        assert isReceiverHalf || !isRoot; // !isParent -> !isRoot

        super.setReplyReceiver((RQReply<T> rep) -> {
            parent.catcher.replyReceived(rep);
        });
        super.setExceptionReceiver((Throwable exc) -> {
            handleErrors(exc);
        });

        if (isRoot && opts.getResponseType() == ResponseType.DIRECT) {
            LocalNode local = (LocalNode)receiver;
            this.root = local;
            this.rootEventId = getEventId();
            // in DIRECT mode, this instance receives RQReplyDirect messages
            RequestEvent.registerRequestEvent(local, this);
            cleanup.add(() -> {
                RequestEvent.removeRequestEvent(local, getEventId());
            });
        } else {
            this.root = null;
            this.rootEventId = 0;
        }
        this.resultsReceiver = resultsReceiver;
        this.targetRanges = Collections.unmodifiableCollection(ranges);
        this.provider = provider;
        this.opts = opts;
        this.failedLinks = new HashSet<>();
        if (isReceiverHalf) {
            this.catcher = new RQCatcher();
        }
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
        RQRequest<T> child = new RQRequest<>(receiver, false, newRQRange,
                this.provider, this.opts, null, this);
        child.root = this.root;
        child.rootEventId = this.rootEventId;
        return child;
    }

    public String toString() {
        return "RQRequest[Opts=" + opts + ", isRoot=" + isRoot
                + ", sender=" + sender + ", receiver=" + receiver
                + ", evId=" + getEventId() + ", root=" + root
                + ", rootEvId=" + rootEventId
                + ", target=" + targetRanges
                + ", failedLinks=" + failedLinks + "]";
    }
    
    @Override
    public boolean beforeRunHook(LocalNode n) {
        return super.beforeRunHook(n);
    }

    public void addFailedLinks(Collection<Node> links) {
        failedLinks.addAll(links);
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
        logger.debug("direct reply received: {}", rep);
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

        /** gaps (subranges that have not yet received any return values) */
        final Set<DdllKeyRange> gaps;

        // basic statistics
        int retransCount = 0;

        TimerEvent expirationTask;
        TimerEvent slowRetransTask; // root node only
        TimerEvent flushTask;

        final RQStrategy strategy;
        final RQResults<?> results;

        public RQCatcher() {
            this.rvals = new ConcurrentSkipListMap<>();
            this.gaps = new HashSet<>();
            gaps.addAll(targetRanges);
            if (isRoot) {
                results = new RQResults(this);
            } else {
                results = null;
            }
            this.strategy = RQStrategy.getRQStrategy(getLocalNode());

            if (isRoot) {
                RetransMode rmode = opts.getRetransMode();
                if (rmode == RetransMode.SLOW || rmode == RetransMode.RELIABLE) {
                    // schedule slow retransmission task
                    long retrans = RQManager.RQ_RETRANS_PERIOD;
                    logger.debug("schedule slow retransmission every {}", retrans);
                    slowRetransTask = EventExecutor.sched(
                            "slowretrans-" + getEventId(),
                            retrans, retrans, () -> {
                        retransmit();
                    });
                    cleanup.add(() -> {
                        slowRetransTask.cancel();
                    });
                }
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
        // XXX: note that wrap-around query ranges such as [10, 5) do not work
        // for now (k-abe)
        private void rqDisseminate() {
            logger.debug("rqDisseminate start: {}", this);
            logger.debug("                   : {}", RQRequest.this);
            Map<Id, List<RQRange>> map = assignDelegates();
            logger.debug("aggregated: {}", map);
            PeerId peerId = getLocalNode().getPeerId();
            ResponseType rtype = opts.getResponseType();

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
                });

            // if we have children, do not wait longer than `expire' period
            long expire = opts.getTimeout();
            if (expire != 0
                    && !this.childMsgs.isEmpty()
                    && (isRoot || opts.getResponseType() == ResponseType.AGGREGATE)) {
                long exp = expire + (isRoot ? 0 : RQManager.RQ_EXPIRATION_GRACE);
                logger.debug("schedule expiration after {}", exp);
                expirationTask = EventExecutor.sched("expire-" + getEventId(),
                        exp, () -> {
                            logger.debug("expired: {}", this);
                            cleanup();
                            notifyResult(null);
                        });
                cleanup.add(() -> {
                    expirationTask.cancel();
                });
            }

            if (!isRoot
                    && !this.childMsgs.isEmpty()
                    && opts.getResponseType() == ResponseType.AGGREGATE) {
                long flush = RQManager.RQ_FLUSH_PERIOD;
                logger.debug("schedule periodic flushing {}", flush);
                flushTask = EventExecutor.sched("flush-" + getEventId(), 
                        flush, flush, () -> flush());
                cleanup.add(() -> {
                    flushTask.cancel();
                });
            }

            // obtain values for local ranges
            CompletableFuture<List<DKRangeRValue<T>>> future
                = rqExecuteLocal(map.get(peerId));
            future.thenAccept((List<DKRangeRValue<T>> rvals) -> {
                logger.debug("rqDisseminate: rvals = {}", rvals);
                if (!isRoot) {
                    switch (rtype) {
                    case NO_RESPONSE:
                        cleanup();
                        break;
                    case DIRECT:
                        addRemoteValues(rvals);
                        break;
                    case AGGREGATE:
                        addRemoteValues(rvals);
                        if (!isCompleted()) {
                            // when no RQReply is sent to the parent, send AckEvent instead.
                            // XXX: if the provider takes long time to finish, it'd be better to send AckEvent firstly.
                            getLocalNode().post(new AckEvent(RQRequest.this, sender));
                        }
                        break;
                    default:
                        throw new Error("shouldn't happen");
                    }
                }
            });
            logger.debug("rqDisseminate finished");
        }

        /**
         * 各 range を subRange に分割し，それぞれ担当ノードを割り当てる．
         * 各ノード毎に割り当てたSubRangeのリストのMapを返す．
         * 
         * @return a map of id and subranges.
         */
         protected Map<Id, List<RQRange>> assignDelegates() {
            Set<Node> maybeFailed = failedLinks;
            // make sure that i'm not failed 
            maybeFailed.remove(getLocalNode());
            List<List<Node>> allNodes = strategy.getRoutingEntries();

            // collect [me, successor)
            List<RQRange> succRanges = new ArrayList<>();
            for (LocalNode v : Arrays.asList(getLocalNode())) {
                succRanges.add(new RQRange(v, v.key, v.succ.key));
            }

            List<Node> actives = allNodes.stream()
                    .flatMap(list -> {
                        Optional<Node> p = list.stream()
                                .filter(node -> true)
                                .findFirst();
                        return streamopt(p);
                    })
                    .collect(Collectors.toList());

            logger.debug("allNodes={}, actives={}", allNodes, actives);
            return targetRanges.stream()
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
            reply.req.cleanup(); // cleanup sender half
            boolean rc = childMsgs.remove(reply.req);
            assert rc;
//            if (reply.isFinal) {
//                finished.add(reply.getSender());
//            }
            addRemoteValues(reply.vals);
        }

        void replyReceived(RQReplyDirect<T> reply) {
            addRemoteValues(reply.vals);
        }

        /**
         * store results of (sub)ranges and flush if appropriate.
         * 
         * @param ranges the ranges.
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
            if (isCompleted() || opts.getResponseType() == ResponseType.DIRECT) {
                // transmit the collected results to the parent node
                flush();
                if (isCompleted() && childMsgs.isEmpty()) {
                    // DIRECTの場合，子ノードからのACKを受信するまでcleanup()してはいけない
                    cleanup();
                }
            } else {
                // fast flushing
                // finished + failed = children ならば flush
                /*Set<Endpoint> eset = new HashSet<>();
                eset.addAll(children);
                eset.removeAll(finished);
                eset.removeAll(failedLinks);
                logger.debug("children={}, finished={}, failed={}, eset={}",
                        children, finished, failedLinks, eset);
                if (eset.size() == 0) {
                    flush();
                }*/
            }
        }

        private void addRemoteValue(RemoteValue<T> rval, Range<DdllKey> range) {
            /*
             * 1. rvalsに，rvalを加える．同一のキーがあれば上書きする．
             *  NavigableMap<DdllKey, DKRangeRValue<?>> rvals;
             *
             * duplicated: [(62.0)!p62..(63.0)!p63)([peer=p62 val=[[val=62]]])
             * duplicated: [(60.0)!p60..(63.0)!p63)([peer=p62 val=[[val=62]]])
             * 2. gaps から r を削る
             */
            if (rvals.containsKey(range.from)) {
                return;
            }

            // find the gap that contains range.from 
            DdllKeyRange gap = gaps.stream()
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
                    DdllKeyRange s = new DdllKeyRange(p.from, true, p.to, false);
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

        /**
         * send the collected results to the parent node. note that the results are
         * cleared after flushing.
         */
        public void flush() {
            if (isRoot) {
                return;
            }
            logger.debug("flush(): {}", this);
            Collection<DKRangeRValue<T>> vals = new ArrayList<>(rvals.values());
            ResponseType rtype = opts.getResponseType();
            switch (rtype) {
            case NO_RESPONSE:
                return;
            case DIRECT:
                if (sender == root) {
                    Event ev = new RQReply<T>(RQRequest.this, vals, true);
                    getLocalNode().post(ev);
                } else {
                    if (rvals.size() > 0) {
                        Event ev = new RQReplyDirect<T>(RQRequest.this, vals);
                        getLocalNode().post(ev);
                    }
                    // send RQReply to parent instead of Ack
                    Event ev = new RQReply<T>(RQRequest.this, null, true);
                    getLocalNode().post(ev);
                }
                break;
            case AGGREGATE:
                if (!vals.isEmpty()) {
                    Event ev = new RQReply<T>(RQRequest.this, vals, isCompleted());
                    getLocalNode().post(ev);
                }
                break;
            default:
                throw new Error("shouldn't happen");
            }
            rvals.clear();
        }

        public RQResults<?> getRQResults() {
            return results;
        }

        boolean isCompleted() {
            // XXX if NO_RESPONSE, request itself completes immediately
            return (TransOptions.responseType(opts) == ResponseType.NO_RESPONSE)
                    || (gaps.size() == 0);
        }
        
        /**
         * (slow) retransmit the range query message for the gap ranges.
         */
        private void retransmit() {
            retransmit(gaps);
        }

        /**
         * slow retransmit a range query message for specified ranges.
         * @param ranges    retransmit ranges
         */
        public void retransmit(Collection<DdllKeyRange> ranges) {
            logger.debug("retransmit: {}, retrans ranges={}", this, ranges);
            //System.err.println("retransmit: " + this + " retrans ranges=" + ranges);
            if (isCompleted()) {
                logger.debug("retransmit: no need");
                return; // nothing to do
            }
//            Collection<RQRange> subRanges =
//                    parentMsg.adjustSubRangesForRetrans(ranges);
//            // 2nd argument is root because we need a root instance here 
//            RQRequest m = newChildInstance(subRanges, true);
//            rqDisseminate();
            retransCount++;
        }

        /**
         * obtain a result value from local node.
         *  
         * @param ranges このノードが担当する範囲のリスト．各範囲はtargetRangeに含まれる
         * @returns CompletableFuture
         */
        private CompletableFuture<List<DKRangeRValue<T>>>
        rqExecuteLocal(List<RQRange> ranges) {
            logger.debug("rqExecuteLocal: list={}", ranges);
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
                        f = provider.get(getLocalNode().key);
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