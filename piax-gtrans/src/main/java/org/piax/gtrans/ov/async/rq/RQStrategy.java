package org.piax.gtrans.ov.async.rq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.piax.common.PeerId;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.LocalEvent;
import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.FTEntry;
import org.piax.gtrans.async.Indirect;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Log;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.async.NodeStrategy;
import org.piax.gtrans.ov.async.rq.RQAdapter.InsertionPointAdapter;
import org.piax.gtrans.ov.async.rq.RQAdapter.KeyAdapter;
import org.piax.gtrans.ov.async.rq.RQEvent.GetLocalValueRequest;
import org.piax.gtrans.ov.async.suzaku.FingerTable;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ring.rq.DdllKeyRange;
import org.piax.util.UniqId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RQStrategy extends NodeStrategy {
    static final Logger logger = LoggerFactory.getLogger(RQStrategy.class);

    public static class RQNodeFactory extends NodeFactory {
        final NodeFactory base;
        public RQNodeFactory(NodeFactory base) {
            this.base = base;
        }
        @Override
        public void setupNode(LocalNode node) {
            base.setupNode(node);
            RQStrategy s = new RQStrategy();
            node.pushStrategy(s);
            s.registerAdapter(new KeyAdapter(null));
            s.registerAdapter(new InsertionPointAdapter(null));
        }
        @Override
        public String toString() {
            return "RQ/" + base.toString();
        }
    }

    /**
     * registered RQadapter
     */
    private Map<Class<? extends RQAdapter<?>>, RQAdapter<?>> adapters
        = new HashMap<>();

    /**
     * query receipt history
     */
    Map<Long, Set<Integer>> queryHistory = new HashMap<>();

    /**
     * query result cache used by
     * {@link org.piax.gtrans.ov.async.rq.RQAdapter.CacheAdapter}
     *  */ 
    Map<PeerId, Map<Long, CompletableFuture<?>>> resultCache = new HashMap<>();

    @Override
    public void handleLookup(Lookup l) {
        l.sendAck(getLocalNode());

        RQRange r = new RQRange(null, l.key).assignId();
        Indirect<Boolean> flag = new Indirect<>(false);
        TransOptions opts = new TransOptions(ResponseType.DIRECT,
                RetransMode.RELIABLE);
        rangeQueryRQRange(Collections.singleton(r),
                new InsertionPointAdapter(rval -> {
                    if (flag.val) {
                        return;
                    }
                    Log.verbose(() -> "handleLookup: rval = " + rval);
                    Event ev;
                    if (rval == null) {
                        // Timeout!
                        ev = new LookupDone(l, null, null);
                    } else {
                        Node[] nodes = rval.getValue();
                        ev = new LookupDone(l, nodes[0], nodes[1]);
                    }
                    n.post(ev);
                    flag.val = true;
                }), opts);
    }

    @Override
    public <T> void rangeQuery(Collection<? extends Range<?>> ranges,
            RQAdapter<T> adapter, TransOptions opts) {
        if (ranges.size() == 0) {
            adapter.handleResult(null);
            return;
        }
        // convert ranges of Comparable<?> into Set<RQRange>
        Set<RQRange> rqranges = ranges.stream().map(r -> {
            RQRange sub = convertToRQRange(r);
            sub.assignId(); // root ID
            return sub;
        }).collect(Collectors.toSet());

        rangeQueryRQRange(rqranges, adapter, opts);
    }

    public <T> void rangeQueryRQRange(Collection<RQRange> ranges,
            RQAdapter<T> adapter, TransOptions opts) {
        n.post(new LocalEvent(n, () -> {
            RQRequest<T> root = new RQRequest<>(n, ranges, adapter, opts);
            root.run();
        }));
    }
    
    private static RQRange convertToRQRange(
            Range<? extends Comparable<?>> range) {
        UniqId id0 = (range.fromInclusive
                ? UniqId.MINUS_INFINITY : UniqId.PLUS_INFINITY);
        UniqId id1 = (range.toInclusive
                ? UniqId.PLUS_INFINITY : UniqId.MINUS_INFINITY);
        return new RQRange(null,
                new DdllKey(range.from, id0),
                new DdllKey(range.to, id1));
    }
    
    public static RQStrategy getRQStrategy(LocalNode node) {
        return (RQStrategy)node.getStrategy(RQStrategy.class);
    }

    public void registerAdapter(RQAdapter<?> adapter) {
        Class<? extends RQAdapter<?>> clazz = adapter.getClazz();
        assert adapters.get(clazz) == null : "duplicate! " + adapter;
        adapters.put(clazz, adapter);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T> RQAdapter<T> getRegisteredAdapter(Class<? extends RQAdapter> clazz) {
        return (RQAdapter<T>) adapters.get(clazz);
    }

    @Override
    public Object getLocalCollectedData(Class<? extends RQAdapter<?>> clazz) {
        RQAdapter<?> adapter = getRegisteredAdapter(clazz);
        return adapter.getCollectedData(n);
    }

    @Override
    public FTEntry getFTEntryToSend(int fromDist, int toDist) {
        FTEntry ent = getLower().getFTEntryToSend(fromDist, toDist);
        if (ent == null) {
            return null;
        }
        final boolean isBackward = fromDist < 0;
        int index = FingerTable.getFTIndex(Math.abs(fromDist));
        int index2 = FingerTable.getFTIndex(Math.abs(toDist));
        Indirect<DdllKey> from = new Indirect<>();
        Indirect<DdllKey> to = new Indirect<>();
        // System.out.println("chk0: isB=" + isBackward + ", index=" + index + ", index2=" + index2);
        // index から index2 までのFTEntryを集約し，FTEntryに格納する
        for (Map.Entry<Class<? extends RQAdapter<?>>, RQAdapter<?>> aEnt
                : adapters.entrySet()) {
            Class<? extends RQAdapter<?>> clazz = aEnt.getKey();
            RQAdapter<?> adapter = aEnt.getValue();
            IntStream stream;
            if (index < index2) {
                stream = IntStream.rangeClosed(index, index2 - 1);
            } else {
                stream = IntStream.rangeClosed(index2 + 1, index);
            }
            List<Object> vals = stream.mapToObj(i -> {
                // XXX: getFIngerTableEntryの実体はSuzakuStrategyにある．
                // RQStrategyがSuzakuStrategyと密接に関連しているので，
                // RQStrategy extends SuzakuStrategyとしたほうが素直かもしれない．
                // (k-abe)
                FTEntry e = getFingerTableEntry(isBackward, i);
                if (e != null) {
                    DdllKeyRange range = e.getRange();
                    if (range != null) {
                        if (from.val == null) from.val = range.from;
                        to.val = range.to;
                    }
                    return e.getLocalCollectedData(adapter.getClazz());
                } else {
                    return null;
                }
            }).filter(Objects::nonNull)
            .collect(Collectors.toList());
            // it is not necessary to do every iteration...
            ent.setRange(new DdllKeyRange(from.val, true, to.val, false));
            ent.putCollectedData(clazz, adapter.reduceCollectedData(vals));
        }
        return ent;
    }

    /*
     * forwadQueryLeft:
     * 
     * - forwardQueryLeft(count)
     * - 範囲の右端の担当ノード n とその右ノードnRightを得る．
     * - forwardQueryLeft(n, nRight, count)を実行
     * 
     * forwardQueryLeft(Node n, Node eRight, int count):
     * - n に対して，InvokeProvider(eRight) メッセージを送信
     *   - InvokeProvider を受信したノードの処理:
     *     - eRight == succ ならば，Provider を実行し，左ノード(nLeft)と右ノード(nRight)を返す．
     *     - そうでなければ，Provider は実行せず，左ノード(nLeft)と右ノード(nRight)を返す（RMismatch)．
     * - RMismatchを受信した場合:
     *   - case1: nRightが，eRightより右 (eRightは削除されている?)
     *     - forwardQueryLeft(n, nRight, count) を実行
     *   - case2: そうではない場合 (SetL未受信?)
     *     - forwardQueryLeft(nRight，eRight, count) を実行
     * - 正常応答を受信した場合:
     *   - ノード数がオーバしたら return
     *   - nLeft が範囲外なら return
     *   - forwardQueryLeft(nLeft, n, count - 1) を実行
     * - 応答を受信しない場合:
     *   - 一つ前のノードからやり直す．
     */

    /**
     * @param range the query range
     * @param num   number of nodes to traverse
     * @param provider the value provider
     * @param opts the TransOptions
     * @param resultsReceiver the function to receive the results
     * @param T the type of returned value
     */
    @Override
    public <T> void forwardQueryLeft(Range<?> range, int num,
            RQAdapter<T> adapter, TransOptions opts) {
        if (num <= 0) {
            throw new IllegalArgumentException("num <= 0");
        }
        FQLParams<T> p = new FQLParams<>();
        {
            p.qid = EventExecutor.random().nextLong();
            p.num = num;
            p.rq = convertToRQRange(range);
            p.adapter = adapter;
            p.opts = opts;
        }
        List<Node> visited = new ArrayList<>();
        startForwardQueryLeft(p, visited);
    }

    // static parameters that does not change while processing the query
    static class FQLParams<T> {
        long qid;
        int num;
        RQRange rq;
        RQAdapter<T> adapter;
        TransOptions opts;
    }

    private <T> void startForwardQueryLeft(FQLParams<T> p, List<Node> visited) {
        RQRange rEnd = new RQRange(null, p.rq.to).assignId();
        LinkedList<Node> trace = new LinkedList<>();
        Indirect<Boolean> flag = new Indirect<>(false);
        // get the right-most node within the range
        rangeQueryRQRange(Collections.singleton(rEnd),
                new InsertionPointAdapter(rval -> {
                    Log.verbose(() -> "startFQL rq rval = " + rval);
                    if (rval != null) {
                        flag.val = true;
                        Node[] nodes = rval.getValue();
                        if (!p.rq.contains(nodes[0].key)) {
                            p.adapter.handleResult(null); // finish!
                            return;
                        }
                        forwardQueryLeft0(p, nodes[0], nodes[1], trace, visited);
                    } else {
                        if (!flag.val) {
                            System.err.println("forwardQueryLeft: couldn't find the start node");
                            p.adapter.handleResult(null);
                        }
                    }
                }), p.opts);
    }

    final static long RETRANS_INTERVAL = 1000;
    private <T> void forwardQueryLeft0(FQLParams<T> p, Node current, 
            Node expectedRight, LinkedList<Node> trace, List<Node> visited) {
        Log.verbose(() -> "current=" + current + ", expected=" + expectedRight
                + ", trace=" + trace);
        boolean circulated = visited.stream()
                .filter(node -> node == current)
                .findAny()
                .isPresent();
        if (expectedRight != null && circulated) {
            logger.debug("forwardQueryLeft: finish (circulated)");
            p.adapter.handleResult(null); // finish!
            return;
        }
        GetLocalValueRequest<T> ev = new GetLocalValueRequest<>(current, 
                expectedRight, p.adapter, p.qid);
        n.post(ev);
        ev.onReply((rep, exc) -> {
            if (exc != null) {
                Log.verbose(() -> "onReply: got " + exc + ", trace=" + trace);
                EventExecutor.sched(RETRANS_INTERVAL, () -> {
                    if (trace.isEmpty()) {
                        startForwardQueryLeft(p, visited);
                    } else {
                        // nodes = {N10, N20, N30, N40}
                        // current = N10, trace = [N40, N30, N20]
                        // if N10 does not respond:
                        // current = N20 (backtrack), trace = [N40, N30]
                        Node last = trace.removeLast();
                        forwardQueryLeft0(p, last, 
                                null // expectedRight=null means special case
                                , trace, visited);
                    }
                });
            } else {
                if (rep.success) {
                    trace.add(current);
                    if (rep.result != null) { // not special case
                        visited.add(current);
                        p.adapter.handleResult(rep.result);
                    }
                    // BUG: rep.pred.key は信頼できるとは限らないため，以下の条件判定はまずい．
                    if (visited.size() >= p.num || !p.rq.contains(rep.pred.key)) {
                        p.adapter.handleResult(null); // finish!
                        return;
                    }
                    forwardQueryLeft0(p, rep.pred, current, trace, visited);
                } else {  // right node mismatch case
                    Log.verbose(() -> "right node mismatch: expected=" 
                            + expectedRight +", actual=" + rep.succ);
                    if (Node.isOrdered(current.key, rep.succ.key, expectedRight.key)) {
                        forwardQueryLeft0(p, rep.succ, expectedRight, trace, visited);
                    } else {
                        forwardQueryLeft0(p, current, rep.succ, trace, visited);
                    }
                }
            }
        });
    }

    <T> CompletableFuture<RemoteValue<T>> getLocalValue(
            RQAdapter<T> received, LocalNode localNode, RQRange r, long qid) {
        // obtain the registered adapter
        RQAdapter<T> rAdapter = getRegisteredAdapter(received.getClass());
        CompletableFuture<T> f;
        try {
            f = rAdapter.getRaw(received, localNode, r, qid);
        } catch (Throwable exc) {
            // if getRaw terminates exceptionally...
            System.err.println("getLocalValue: got " + exc);
            exc.printStackTrace();
            RemoteValue<T> rval = new RemoteValue<>(getLocalNode().peerId, exc);
            return CompletableFuture.completedFuture(rval);
        }
        // RQAdapter.getRaw may be executed by a separate thread.
        // To avoid scattering mutual exclusion code, we let the event executor
        // thread handle the successive jobs.
        CompletableFuture<T> ret = unparallel(f);
        return ret.handle((T val, Throwable exc) -> {
            RemoteValue<T> rval;
            if (exc != null) {
                rval = new RemoteValue<>(getLocalNode().peerId, exc);
            } else {
                rval = new RemoteValue<>(getLocalNode().peerId, val);
            }
            return rval;
        });
    }

    /**
     * 別スレッドで実行されるCompletableFutureが終了したら
     * event executor thread で実行される CompletableFuture を返す．
     */
    private <T> CompletableFuture<T> unparallel(CompletableFuture<T> f) {
        CompletableFuture<T> ret = new CompletableFuture<>();
        f.whenComplete((T val, Throwable exc) -> {
            LocalEvent ev = new LocalEvent(n, () -> {
                if (exc != null) {
                    ret.completeExceptionally(exc);
                } else {
                    ret.complete(val);
                }
            });
            n.post(ev);
        });
        return ret;
    }
}
