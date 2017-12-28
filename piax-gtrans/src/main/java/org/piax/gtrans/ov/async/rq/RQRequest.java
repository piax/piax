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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.piax.common.Id;
import org.piax.common.PeerId;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.ReturnValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.StreamingRequestEvent;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.FTEntry;
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
 * target nodes. this class also contains {@link #obstacles} field, which
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
    public static int RQ_RETRY_SUCCESSOR_FAILURE_DELAY = 1000;

    public final long qid;
    Node root;       // DIRECT only
    int rootEventId; // DIRECT only
    // XXX: note that wrap-around query ranges such as [10, 5) do not work
    // for now (k-abe)
    public final Collection<RQRange> targetRanges;
    public final RQAdapter<T> adapter;
    final TransOptions opts;
    final boolean isRoot;

    /**
     * failed links. this field is used for avoiding and repairing dead links.
     * XXX: NOT USED FOR NOW
     */
    final Set<Node> obstacles;

    public transient RQCatcher catcher; // receiver half only

    enum SPECIAL {
        PADDING;
    }

    /**
     * create a root RQRequest.
     * 
     * @param ranges set of query ranges
     * @param query an object sent to all the nodes within the query ranges
     * @param opts the transport options.
     */
    /*
     * isRoot -> isReceiverHalf
     * isRoot <-> resultReceiver != null
     * !isReceiverHalf -> resultReceiver == null
     */
    RQRequest(Node receiver, Collection<RQRange> ranges,
            RQAdapter<T> adapter, TransOptions opts) {
        super(receiver, true);
        this.isRoot = true;
        assert adapter.resultsReceiver != null;
        assert isReceiverHalf; // isRoot -> isReceiverHalf

        super.setReplyReceiver((RQReply<T> rep) -> {
            assert false;
        });
        super.setExceptionReceiver((Throwable exc) -> {
            assert false;
        });

        this.qid = EventExecutor.random().nextLong();
        this.root = null;  // overridden by DirectResponder at root node
        this.rootEventId = 0; // overridden by DirectResponder at root node
        this.targetRanges = Collections.unmodifiableCollection(ranges);
        this.adapter = adapter;
        this.opts = opts;
        this.obstacles = new HashSet<>();
    }

    /**
     * create a sender-half of child RQRequest from parent instance.
     * <p>
     * this method is used both at intermediate nodes and at root node.
     * 
     * @param parent
     * @param receiver
     * @param newRQRange new RQRange for the child RQMessage
     */
    private RQRequest(RQRequest<T> parent, Node receiver,
            Collection<RQRange> newRanges, Consumer<Throwable> errorHandler) {
        super(receiver, false);
        this.isRoot = false;
        super.setReplyReceiver((RQReply<T> rep) -> {
            parent.catcher.replyReceived(rep);
        });
        super.setExceptionReceiver(errorHandler);
        this.qid = parent.qid;
        this.root = parent.root;
        this.rootEventId = parent.rootEventId;
        this.targetRanges = newRanges;
        this.adapter = parent.adapter;
        this.opts = parent.opts;
        this.obstacles = parent.obstacles;
        this.catcher = parent.catcher;
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
        if (isUseAck(opts)) {
            return NetworkParams.ACK_TIMEOUT;
        }
        return 0;
    }

    @Override
    protected long getReplyTimeoutValue() {
        if (opts.getResponseType() == ResponseType.NO_RESPONSE
                ||
            // in DIRECT mode, non root node does not receive RQDirectReply
            opts.getResponseType() == ResponseType.DIRECT && !isRoot) {
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
        // check if we've received the same query
        // targetRanges に含まれる各部分範囲について，受信済みかどうかを個別に判定しているが，
        // 現在の実装では，1つのRQRequestで受信済みと非受信済みの範囲が混在することはない．
        RQStrategy strategy = RQStrategy.getRQStrategy(getLocalNode());
        Set<Integer> history = strategy.queryHistory.computeIfAbsent(qid,
                q -> {
                    EventExecutor.sched("purge_qh-" + qid,
                            opts.getTimeout() + RQManager.RQ_EXPIRATION_GRACE,
                            () -> strategy.queryHistory.remove(qid));
                    return new HashSet<>();
                });
        List<RQRange> filtered = targetRanges.stream()
            .filter(r -> {
                int lastId = r.ids[r.ids.length - 1];
                boolean received = history.contains(lastId);
                return !received;
            }).collect(Collectors.toList());
        if (filtered.isEmpty()) {
            logger.debug("already received");
            return;
        }
        if (catcher == null) {
            // note that catcher is transient
            this.catcher = new RQCatcher(filtered);
        }
        catcher.rqDisseminate(catcher.gaps);
    }
 
    public void receiveReply(RQReplyDirect<T> rep) {
        logger.debug("RQRequest: direct reply received: {}", rep);
        catcher.replyReceived(rep);
    }

    @Override
    protected Event clone() {
        RQRequest<?> ev = (RQRequest<?>)super.clone();
        ev.catcher = null;
        return ev;
    }

    private static boolean isUseAck(TransOptions opt) {
        RetransMode mode = opt.getRetransMode();
        return mode == RetransMode.NONE_ACK
                || mode == RetransMode.FAST
                || mode == RetransMode.RELIABLE;
    }

    public boolean isReceiverHalf() {
		return this.isReceiverHalf;
    }

    /**
     * A class for storing results of a range query used by parent nodes.
     */
    public class RQCatcher {
        
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
        final List<RQRange> gaps;

        int retransCount = 0;

        final RQStrategy strategy;
        final Responder responder;
        final RQResults<T> results;

        public RQCatcher(Collection<RQRange> ranges) {
            this.rvals = new ConcurrentSkipListMap<>();
            this.gaps = new ArrayList<>(ranges);
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

        private void rqDisseminate(List<RQRange> ranges) {
        		boolean sendChild = true;
        		if (adapter.getHook() != null) {
        			sendChild = adapter.getHook().hook(RQRequest.this);
        		}
        		rqDisseminate0(ranges, sendChild);
        }

        public void rqDisseminateWithoutHook(List<RQRange> ranges) {
        		rqDisseminate0(ranges, true);
        }
        		         
        private void rqDisseminate0(List<RQRange> ranges, boolean sendChild) {
            if (logger.isTraceEnabled()) {
                logger.trace("rqDisseminate start: {}", this);
                logger.trace("                     {}", RQRequest.this);
            }

            Set<Integer> history = strategy.queryHistory.get(qid);
            if (history == null) {
                // retransmission too late! 
                return;
            }
            ranges.stream().forEach(r -> history.addAll(Arrays.asList(r.ids)));

            List<FTEntry> ftents = getTopStrategy().getRoutingEntries();
            if (ftents.isEmpty()) {
                logger.trace("no routing entry available!");
                return;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("rqd#ftents={}", ftents);
            }
            List<DKRangeRValue<T>> locallyResolved = new ArrayList<>();
            ranges = adapter.preprocess(ranges, ftents, locallyResolved);
            {
                List<RQRange> ranges0 = ranges;
                logger.trace("rqd#ranges={}",  ranges0);
                logger.trace("rqd#locally={}", locallyResolved);
            }
            assert gaps != null && !gaps.isEmpty();
            locallyResolved.stream().forEach(dkr -> {
                addRemoteValue(dkr.getRemoteValue(), dkr);
            });

            // assign a delegate node for each range
            Map<Id, List<RQRange>> map = assignDelegates(ranges);
            if (logger.isTraceEnabled()) {
                logger.trace("aggregated: {}", map);
            }
            PeerId peerId = getLocalNode().getPeerId();

            /*
             * send aggregated requests to children.
             */
            if (sendChild) {
            	map.entrySet().stream()
                .filter(ent -> !ent.getKey().equals(peerId))
                .forEach(ent -> {
                    List<RQRange> sub = ent.getValue();
                    Node dlg = sub.get(0).getNode();
                    RQRequest<T> m = new RQRequest<>(RQRequest.this, dlg, sub, 
                            (Throwable th) -> {
                                logger.debug("{} for {}", th, RQRequest.this);
                                getLocalNode().addMaybeFailedNode(dlg);
                                RetransMode mode = opts.getRetransMode();
                                if (mode == RetransMode.FAST || mode == RetransMode.RELIABLE) {
                                    if (dlg == getLocalNode().succ) {
                                        logger.debug("start fast retransmission! (delayed) {}", sub);
                                        EventExecutor.sched(
                                                "rq-retry-successor-failure",
                                                RQ_RETRY_SUCCESSOR_FAILURE_DELAY,
                                                () -> rqDisseminate(sub));
                                    } else {
                                        logger.debug("start fast retransmission! {}", sub);
                                        rqDisseminate(sub);
                                    }
                                }
                            });
                    logger.debug("send to child: {}", m);
                    this.childMsgs.add(m);
                    m.cleanup.add(() -> {
                        boolean rc = this.childMsgs.remove(m);
                        assert rc;
                    });
                    getLocalNode().post(m);
                    cleanup.add(() -> m.cleanup());
                });
            }
            // obtain values for local ranges
            CompletableFuture<List<DKRangeRValue<T>>> future = null;
            if (adapter.getHook() != null) {
            		future = adapter.getHook().executeLocal(RQRequest.this, map.get(peerId));
            } else {
            		future = rqExecuteLocal(map.get(peerId));
            }
            future.thenAccept((List<DKRangeRValue<T>> rvals) -> {
                addRemoteValues(rvals);
                responder.rqDisseminateFinish();
            }).exceptionally((exc) -> {
                System.err.println("addRemoteValues completes exceptionally");
                exc.getCause().printStackTrace();
                return null; // or System.exit(1);
            });
            if (adapter.getHook() != null) {
            		adapter.getHook().addHistory(RQRequest.this);
            }
            logger.trace("rqDisseminate finished");
        }

        /**
         * gapの各範囲を部分範囲に分割し，それぞれ担当ノードを割り当てる．
         * 各ノード毎に割り当てたRQRangeのリストのMapを返す．
         * 
         * @return a map of id and RQRanges
         */
        protected Map<Id, List<RQRange>> assignDelegates(List<RQRange> ranges) {
            LocalNode local = getLocalNode(); 
            // collect [me, successor)
            List<RQRange> succRanges = new ArrayList<>();
            local.getSiblings().stream()
                .forEach(v -> succRanges.add(new RQRange(v, v.key, v.succ.key)));
            List<Node> actives = local.getActiveNodeStream()
                    .collect(Collectors.toList());
            logger.debug("actives={}", actives);
            return ranges.stream()
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
                Node d = getLocalNode().getClosestPredecessor(queryRange.from);
                RQRange r;
                if (first == null) {
                    // OK: covers the case where queryRange is just a point
                    r = new RQRange(d, queryRange, queryRange.ids);
                } else {
                    r = new RQRange(d, queryRange.from, first.key, queryRange.ids);
                }
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
                // cleanup sender half of the corresponding request
                reply.req.cleanup();
            }
            addRemoteValues(reply.vals);
        }

        void replyReceived(RQReplyDirect<T> reply) {
            addRemoteValues(reply.vals);
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
            if (isCompleted()) {
                cleanup();
            }
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
            List<Range<DdllKey>> retains = gap.retain(range, null);
            // add the remaining ranges to gaps
            if (retains != null) {
                for (Range<DdllKey> p : retains) {
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
            assert isRoot;
            if (rval == null || rval.getValue() != SPECIAL.PADDING) {
                adapter.handleResult(rval);
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
        public CompletableFuture<List<DKRangeRValue<T>>>
        rqExecuteLocal(List<RQRange> ranges) {
            logger.trace("rqExecuteLocal: ranges={}", ranges);
            // results of locally-resolved ranges
            List<DKRangeRValue<T>> rvals = new ArrayList<>();
            if (ranges == null) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> futures = ranges.stream()
                .map(r -> invoke(r, rvals)) // CompletableFuture<Void>
                .reduce((a, b) -> a.runAfterBoth(b, () -> {}))
                .orElse(null);
            CompletableFuture<List<DKRangeRValue<T>>> rc = new CompletableFuture<>();
            futures.thenRun(() -> rc.complete(rvals));
            return rc;
        }

        private CompletableFuture<Void> invoke(
                RQRange r, List<DKRangeRValue<T>> rvals) {
            return strategy.getLocalValue(adapter,
                    (LocalNode)r.getNode(), r, qid)
                    // on provider completion, adds the value to rvals 
                    .thenAccept(rval -> rvals.add(new DKRangeRValue<T>(rval, r)));
        }

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
                            if (isRoot) {
                                notifyResult(null);
                            }
                        });
                cleanup.add(() -> expirationTask.cancel());
            }
            protected void startSlowRetransTask() {
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
                                // reassign ID!
                                List<RQRange> subst = gaps.stream().map(r -> 
                                        new RQRange(r.getNode(), r.from, r.to)
                                        .assignId())
                                        .collect(Collectors.toList());
                                logger.debug("start slow retransmit: subst={}",
                                        subst);
                                rqDisseminate(subst);
                            });
                    cleanup.add(() -> {
                        slowRetransTask.cancel();
                    });
                }
            }
        }

        /*
         * Message Sequences:
         * 
         * NO_RESPONSE:
         * 
         *   ROOT----RQRequest---->CHILD1                 CHILD2
         *     |                     |------RQRequest------>|
         */
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

        /*
         * AGGREGATE:
         *
         *   ROOT                 CHILD1                 CHILD2
         *     |-----RQRequest------>|                      |
         *     |<------(Ack)---------|------RQRequest------>|
         *     |                     |<--------(Ack)--------|
         *     |<-----RQReply--------|<-------RQReply-------|
         *
         *  - Ack is sent to the parent only if the node cannot send RQReply
         *    quickly.
         */
        class AggregateResponder extends Responder {
            TimerEvent sendReplyTask;
            boolean notAcked = true;
            public AggregateResponder() {
                if (isRoot) {
                    startSlowRetransTask();
                } else {
                    // schedule calling flush() periodically.
                    // if use ACK, schedule calling flush() at time1.
                    long time2 = RQManager.RQ_FLUSH_PERIOD;
                    long time1 = isUseAck(opts) ? NetworkParams.SEND_ACK_TIME : time2;
                    sendReplyTask = EventExecutor.sched("sendreplytask-" + getEventId(), 
                            time1, time2, () -> flush());
                    cleanup.add(() -> {
                        sendReplyTask.cancel();
                    });
                }
            }
            @Override
            void rqDisseminateFinish() {
                if (!isCompleted()) {
                    startExpirationTask();
                }
            }
            @Override
            void onReceiveValues() {
                if (!isRoot && isCompleted()) {
                    flush();
                    // cleanup() is called by addRemoteValues()
                    //cleanup();
                }
            }
            private void flush() {
                if (notAcked || !rvals.isEmpty()) {
                    // if we don't copy, we'll send an empty list
                    Collection<DKRangeRValue<T>> copy = new ArrayList<>(rvals.values());
                    Event ev = new RQReply<T>(RQRequest.this, copy, isCompleted());
                    getLocalNode().post(ev);
                    rvals.clear();
                    notAcked = false;
                }
            }
        }

       /* 
        * DIRECT:
        *
        *   ROOT                 CHILD1                 CHILD2
        *     |---RQRequest-------->|                      |
        *     |<----RQReplyDirect---|------RQRequest------>|
        *     |                     |<--------Ack----------|
        *     |<-------------------------RQReplyDirect-----|
        */ 
        class DirectResponder extends Responder {
            TimerEvent sendAckTimer;
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
                    startSlowRetransTask();
                } else if (isUseAck(opts)) {
                    if (sender != root) {
                        getLocalNode().post(new AckEvent(RQRequest.this, sender));
                    } else {
                        sendAckTimer = EventExecutor.sched("sendacktimer-" + getEventId(), 
                                NetworkParams.SEND_ACK_TIME, () -> {
                                    getLocalNode().post(
                                            new AckEvent(RQRequest.this, sender));
                        });
                        cleanup.add(() -> {
                            sendAckTimer.cancel();
                        });
                    }
                }
            }
            @Override
            void rqDisseminateFinish() {
                if (isRoot && !isCompleted()) {
                    startExpirationTask();
                }
            }
            @Override
            void onReceiveValues() {
                if (!isRoot && rvals.size() > 0) {
                    Event ev = new RQReplyDirect<T>(RQRequest.this, rvals.values());
                    getLocalNode().post(ev);
                    if (sendAckTimer != null) {
                        sendAckTimer.cancel();
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
