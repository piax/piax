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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.piax.common.Endpoint;
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
 * an abstract class representing a message used for propagating range queries.
 * <p>
 * this class contains various data that are required to be transmitted to the
 * target nodes. this class also contains {@link #failedLinks} field, which
 * represents a set of failed nodes that are found while processing the range
 * query.
 * <p>
 * this class also manages (partial) results returned from child nodes.
 * 
 */
public class RQRequest extends StreamingRequestEvent<RQRequest, RQReply> {
    /*--- logger ---*/
    private static final Logger logger =
            LoggerFactory.getLogger(RQRequest.class);
    private static final long serialVersionUID = 1L;

    Node root;       // DIRECT only
    int rootEventId; // DIRECT only

    /** the target ranges, that is not modified */
    protected final Collection<RQRange> targetRanges;
    /* query contents */
    final Object query;
    final TransOptions opts;
    final boolean isRoot;
    transient final Consumer<RemoteValue<?>> resultsReceiver;  // root only

    /**
     * failed links. this field is used for avoiding and repairing dead links.
     */
    final Set<Node> failedLinks;

    transient RQCatcher catcher; // parent only

    // for handling the left-most part of the query range
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
    public RQRequest(Node receiver, Collection<RQRange> ranges, Object query,
            TransOptions opts, Consumer<RemoteValue<?>> resultReceiver) {
        this(receiver, ranges, query, opts, true, resultReceiver, null);
    }

    /*
     * isRoot -> isParent
     * isRoot <-> resultReceiver != null
     * !isParent -> resultReceiver == null
     */
    private RQRequest(Node receiver, 
            Collection<RQRange> ranges, Object query,
            TransOptions opts, boolean isParent,
            Consumer<RemoteValue<?>> resultReceiver,
            RQRequest parent) {
        super(receiver, isParent, (RQReply rep) -> {
            logger.debug("reply received: {}", rep);
            rep.req.cleanup();
            parent.catcher.replyReceived(rep);
        }, (Throwable exc) -> {
            logger.debug("got exception: {}", exc);
        });
        this.isRoot = resultReceiver != null;
        assert !isRoot || isParent; // isRoot -> isParent
        assert isParent || !isRoot; // !isParent -> !isRoot
        if (isRoot && opts.getResponseType() == ResponseType.DIRECT) {
            this.root = receiver;
            this.rootEventId = getEventId();
            // in DIRECT mode, this instance receives RQReplyDirect messages
            RequestEvent.registerRequestEvent((LocalNode)receiver, this);
        } else {
            this.root = null;
            this.rootEventId = 0;
        }
        this.resultsReceiver = resultReceiver;
        this.targetRanges = Collections.unmodifiableCollection(ranges);
        this.query = query;
        this.opts = opts;
        this.failedLinks = new HashSet<>();
        if (isParent) {
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
    private RQRequest newChildInstance(Node receiver,
            Collection<RQRange> newRQRange) {
        RQRequest child = new RQRequest(receiver, newRQRange, this.query, this.opts,
                false, null, this);
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
    public void beforeRunHook(LocalNode n) {
        super.beforeRunHook(n);
    }

    public void addFailedLinks(Collection<Node> links) {
        failedLinks.addAll(links);
    }

    public Collection<RQRange> getTargetRanges() {
        return targetRanges;
    }

    public Object getQuery() {
        return query;
    }

    @Override
    public void run() {
        if (catcher == null) {
            this.catcher = new RQCatcher();
        }
        catcher.rqDisseminate();
    }
 
    public void receiveReply(RQReplyDirect rep) {
        logger.debug("direct reply received: {}", rep);
        catcher.replyReceived(rep);
    }

    @Override
    public void cleanup() {
        assert !isParent;
        super.cleanup();
        if (catcher != null) {
        }
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
        final RQResults<?> results;

        /** return values */
        // XXX: return value 1つづつに DdllKeyRange を保持するのはメモリ効率が悪い．
        // XXX: 中継ノードから親ノードに渡すときに，連続するKeyRangeをまとめると良い．
        final NavigableMap<DdllKey, DKRangeRValue<?>> rvals;

        /** messages sent to children */
        final Set<RQRequest> childMsgs = new HashSet<>();

        /** gaps (subranges that have not yet received any return values) */
        final NavigableMap<DdllKey, DdllKeyRange> gaps;

        /** set of Endpoint of the destination of child messages */
        //final Set<Endpoint> children = new HashSet<>();
        /** subset of children that has completed the request */
        final Set<Endpoint> finished = new HashSet<>();

        private boolean disposed = false;

        // basic statistics
        int rcvCount = 0;
        int retransCount = 0;

        TimerEvent expirationTask;
        TimerEvent slowRetransTask; // root node only
        TimerEvent flushTask;

        final RQStrategy strategy;

        public RQCatcher() {
            this.rvals = new ConcurrentSkipListMap<>();
            this.gaps = new ConcurrentSkipListMap<>();
            for (DdllKeyRange range : targetRanges) {
                gaps.put(range.from, range);
            }
            int expire = (int) TransOptions.timeout(opts);
            if (isRoot) {
                results = new RQResults(this);
            } else {
                results = null;
            }
            this.strategy = RQStrategy.getRQStrategy(getLocalNode());

            /* 
             * schedule tasks:
             * (1) expiration task (purge after expiration period)
             * (2) (slow) retransmission task (root node only)
             * (3) flush task (non root intermediate node only)
             */
            if (expire != 0) {
                // この処理は child message があるときだけで良いはず
                long exp = expire + (isRoot ? 0 : RQManager.RQ_EXPIRATION_GRACE);
                logger.debug("schedule expiration after {}", exp);
                expirationTask = EventExecutor.sched("expire-" + getEventId(),
                        exp, () -> {
                            logger.debug("RQReturn: expired: {}", this);
                            dispose();
                            if (resultsReceiver != null) {
                                resultsReceiver.accept(RQStrategy.END_OF_RESULTS);
                            }
                        });
            }
            if (isRoot) {
                // retransmission task
                RetransMode rmode = opts.getRetransMode();
                if (rmode == RetransMode.SLOW || rmode == RetransMode.RELIABLE) {
                    long retrans = RQManager.RQ_RETRANS_PERIOD;
                    logger.debug("schedule slow retransmission every {}", retrans);
                    slowRetransTask = EventExecutor.sched(
                            "slowretrans-" + getEventId(),
                            retrans, retrans, () -> {
                        retransmit();
                    });
                }
            } else if (opts.getResponseType() == ResponseType.AGGREGATE) {
                // flush task
                long flush = RQManager.RQ_FLUSH_PERIOD;
                logger.debug("schedule periodic flushing {}", flush);
                flushTask = EventExecutor.sched("flush-" + getEventId(), 
                        flush, flush, () -> {
                            flush();
                        });
            }
        }

        @Override
        public String toString() {
            return "RQCatcher[ID=" + getEventId()
            + ", rvals="
            + (rvals.size() > 10 ? "(" + rvals.size() + " entries)"
                    : rvals.values())
            + ", gaps=" + gaps.values() + ", childMsgs="
            + childMsgs + ", rcvCount=" + rcvCount
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
                    RQRequest m = newChildInstance(dlg, sub);
                    logger.debug("send to child: {}", m);
                    this.childMsgs.add(m);
                    getLocalNode().post(m);
                });

            // obtain values for local ranges
            List<DKRangeRValue<?>> rvals = rqExecuteLocal(map.get(peerId));
            logger.debug("rqDisseminate: rvals = {}", rvals);
            
            if (!isRoot) {
                ResponseType rtype = opts.getResponseType();
                switch (rtype) {
                case NO_RESPONSE:
                    dispose();
                    break;
                case DIRECT:
                    addRemoteValues(rvals);
                    //flush();
                    //dispose();
                    break;
                case AGGREGATE:
                    addRemoteValues(rvals);
                    if (!isCompleted()) {
                        // when no RQReply is sent to the parent, send AckEvent instead.
                        getLocalNode().post(new AckEvent(RQRequest.this, sender));
                    }
                    break;
                default:
                    throw new Error("shouldn't happen");
                }
            }
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
            RQRange qr = queryRange;
            // included = nodes that are contained in the queryRange
            List<Node> included = actives.stream()
                    .filter(node -> qr.contains(node.key))
                    .sorted()
                    .collect(Collectors.toList());
            logger.debug("qr={}, included={}", qr, included);

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
                logger.debug("added to list: {}", r);
            }

            for (int i = 0; i < included.size(); i++) {
                Node ent = included.get(i);
                Node next = (i == included.size() - 1) ? null : included.get(i + 1);
                DdllKey from = (i == 0) ? queryRange.from : ent.key;
                DdllKey to = (i == included.size() - 1) ? queryRange.to: next.key; 
                RQRange r = new RQRange(ent, from, to);
                r.assignSubId(queryRange);
                list.add(r);
                logger.debug("added to list2: {}", r);
            }
            logger.debug("assignDelegate: QR={}, returns {}", queryRange, list);
            return list;
        }

        private void dispose() {
            if (disposed) {
                return;
            }
            logger.debug("dispose: {}", this);
            childMsgs.clear();
            expirationTask.cancel();
            if (slowRetransTask != null) {
                slowRetransTask.cancel();
                slowRetransTask = null;
            }
            if (flushTask != null) {
                flushTask.cancel();
                flushTask = null;
            }
            disposed = true;
        }

        /**
         * called when a reply message is received.
         * 
         * @param reply the reply message.
         */
        void replyReceived(RQReply reply) {
            boolean rc = childMsgs.remove(reply.req);
            assert rc;
            incrementRcvCount();
//            if (reply.isFinal) {
//                finished.add(reply.getSender());
//            }
            addRemoteValues(reply.vals);
        }

        void replyReceived(RQReplyDirect reply) {
            incrementRcvCount();
            addRemoteValues(reply.vals);
        }

        /**
         * store results of (sub)ranges and flush if appropriate.
         * 
         * @param ranges the ranges.
         */
        private void addRemoteValues(Collection<DKRangeRValue<?>> ranges) {
            if (ranges == null) {
                // ACKの代わりにRQReplyを受信した場合
                return;
            }
            for (DKRangeRValue<?> range : ranges) {
                addRemoteValue(range.getRemoteValue(), range);
            }
            logger.debug("gaps={}", gaps);
            if (isCompleted() || opts.getResponseType() == ResponseType.DIRECT) {
                // transmit the collected results to the parent node
                flush();
                if (childMsgs.isEmpty()) {
                    // DIRECTの場合，子ノードからのACKを受信するまでdispose()してはいけない
                    dispose();
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

        private void addRemoteValue(RemoteValue<?> rval, Range<DdllKey> range) {
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
            Map.Entry<DdllKey, DdllKeyRange> ent = gaps.entrySet().stream()
                .filter(e -> e.getValue().contains(range.from))
                .findAny()
                .orElse(null);
            if (ent == null) {
                logger.info("no gap instance: {} in {}", range.from, this);
                return;
            }
            CircularRange<DdllKey> gap = ent.getValue();
            // delete the range r from gaps
            gaps.remove(ent.getKey());
            List<CircularRange<DdllKey>> retains = gap.retain(range, null);
            // add the remaining ranges to gaps
            if (retains != null) {
                for (CircularRange<DdllKey> p : retains) {
                    DdllKeyRange s = new DdllKeyRange(p.from, true, p.to, false);
                    gaps.put(s.from, s);
                }
            }
            logger.debug("addRV: gap={}, r={}, retains={}, gaps={}", gap, range,
                    retains, gaps);
            rvals.put(range.from, new DKRangeRValue<>(rval, range));

            if (isRoot) {
                Object v = rval.getValue();
                if (v instanceof MVal) {
                    // in the future, MVal should be removed
                    MVal mval = (MVal)v;
                    for (ReturnValue<Object> o : mval.vals) {
                        notifyResult(new RemoteValue<Object>(
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
        
        private void notifyResult(RemoteValue<?> rval) {
            if (rval == null || rval.getValue() != SPECIAL.PADDING) {
                resultsReceiver.accept(rval);
            }
        }

        /**
         * send the collected results to the parent node. note that the results are
         * cleared after flushing.
         */
        public void flush() {
            logger.debug("flush(): {}", this);
            if (isRoot) {
                return;
            }
            Collection<DKRangeRValue<?>> vals = new ArrayList<>(rvals.values());
            ResponseType rtype = opts.getResponseType();
            switch (rtype) {
            case NO_RESPONSE:
                return;
            case DIRECT:
                if (sender == root) {
                    Event ev = new RQReply(RQRequest.this, vals, true);
                    getLocalNode().post(ev);
                } else {
                    if (rvals.size() > 0) {
                        Event ev = new RQReplyDirect(RQRequest.this, vals);
                        getLocalNode().post(ev);
                    }
                    // send RQReply to parent instead of Ack
                    Event ev = new RQReply(RQRequest.this, null, true);
                    getLocalNode().post(ev);
                }
                break;
            case AGGREGATE:
                Event ev = new RQReply(RQRequest.this, vals, isCompleted());
                getLocalNode().post(ev);
                break;
            default:
                throw new Error("shouldn't happen");
            }
            rvals.clear();
        }

        public RQResults<?> getRQResults() {
            return results;
        }

        void incrementRcvCount() {
            rcvCount++;
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
            retransmit(gaps.values());
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
         * execute the execQuery at local node
         * @param list このノードが担当する範囲のリスト．各範囲はtargetRangeに含まれる
         * @param rvals
         */
        public List<DKRangeRValue<?>> rqExecuteLocal(List<RQRange> list) {
            logger.debug("rqExecuteLocal: list={}", list);
            // results of locally-resolved ranges
            List<DKRangeRValue<?>> rvals = new ArrayList<>();
            if (list == null) {
                return rvals;
            }
            for (RQRange r : list) {
                Object val = SPECIAL.PADDING;
                if (r.contains(r.getNode().key)) {
                    val = getLocalNode().key;
                }
                RemoteValue<?> rval = new RemoteValue<>(getLocalNode().peerId,
                        val);
                rvals.add(new DKRangeRValue<>(rval, r));
            }
            return rvals;
        }
    }

    public static class MVal implements Serializable {
        private static final long serialVersionUID = 1L;
        public List<ReturnValue<Object>> vals =
                new ArrayList<ReturnValue<Object>>();

        public String toString() {
            return "" + vals;
        }
    }
}