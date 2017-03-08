package org.piax.gtrans.ov.async.rq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
import org.piax.gtrans.async.Event.ReplyEvent;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.Indirect;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Log;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.async.NodeStrategy;
import org.piax.gtrans.ov.async.rq.RQValueProvider.InsertionPointProvider;
import org.piax.gtrans.ov.async.rq.RQValueProvider.KeyProvider;
import org.piax.gtrans.ov.ddll.DdllKey;
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
            s.registerValueProvider(new KeyProvider());
            s.registerValueProvider(new InsertionPointProvider());
        }
        @Override
        public String toString() {
            return "RQ/" + base.toString();
        }
    }

    /**
     * registered RQValueProvider
     */
    private Map<Class<? extends RQValueProvider<?>>, RQValueProvider<?>> providers
        = new HashMap<>();

    /**
     * query receipt history
     */
    Map<Long, Set<Integer>> queryHistory = new HashMap<>();

    /**
     * query result cache used by
     * {@link org.piax.gtrans.ov.async.rq.RQValueProvider.CacheProvider}
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
                new InsertionPointProvider(), opts,
                rval -> {
                    if (flag.val) {
                        return;
                    }
                    Log.verbose(() -> "handleLookup: rval = " + rval);
                    Node[] nodes = rval.getValue();
                    Event ev = new LookupDone(l, nodes[0], nodes[1]);
                    n.post(ev);
                    flag.val = true;
                });
    }

    @Override
    public <T> void rangeQuery(Collection<? extends Range<?>> ranges,
            RQValueProvider<T> provider, TransOptions opts,
            Consumer<RemoteValue<T>> resultsReceiver) {
        if (ranges.size() == 0) {
            resultsReceiver.accept(null);
            return;
        }
        // convert ranges of Comparable<?> into Set<RQRange>
        Set<RQRange> rqranges = ranges.stream().map(r -> {
            RQRange sub = convertToRQRange(r);
            sub.assignId(); // root ID
            return sub;
        }).collect(Collectors.toSet());

        rangeQueryRQRange(rqranges, provider, opts, resultsReceiver);
    }

    public <T> void rangeQueryRQRange(Collection<RQRange> ranges,
            RQValueProvider<T> provider, TransOptions opts,
            Consumer<RemoteValue<T>> resultsReceiver) {
        n.post(new LocalEvent(n, () -> {
            RQRequest<T> root = new RQRequest<>(n, ranges, provider, opts,
                    resultsReceiver);
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

    public void registerValueProvider(RQValueProvider<?> provider) {
        @SuppressWarnings("unchecked")
        Class<? extends RQValueProvider<?>> clazz =
                (Class<? extends RQValueProvider<?>>) provider.getClass();
        providers.put(clazz, provider);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T> RQValueProvider<T>
    getProvider(Class<? extends RQValueProvider> clazz) {
        return (RQValueProvider<T>) providers.get(clazz);
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
            RQValueProvider<T> provider, TransOptions opts,
            Consumer<RemoteValue<T>> resultsReceiver) {
        if (num <= 0) {
            throw new IllegalArgumentException("num <= 0");
        }
        FQLParams<T> p = new FQLParams<>();
        {
            p.qid = EventExecutor.random().nextLong();
            p.num = num;
            p.rq = convertToRQRange(range);
            p.provider = provider;
            p.opts = opts;
            p.resultsReceiver = resultsReceiver;
        }
        List<Node> visited = new ArrayList<>();
        startForwardQueryLeft(p, visited);
    }

    // static parameters that does not change while processing the query
    static class FQLParams<T> {
        long qid;
        int num;
        RQRange rq;
        RQValueProvider<T> provider;
        TransOptions opts;
        Consumer<RemoteValue<T>> resultsReceiver;
    }

    private <T> void startForwardQueryLeft(FQLParams<T> p, List<Node> visited) {
        RQRange rEnd = new RQRange(null, p.rq.to).assignId();
        LinkedList<Node> trace = new LinkedList<>();
        Indirect<Boolean> flag = new Indirect<>(false);
        // get the right-most node within the range
        rangeQueryRQRange(Collections.singleton(rEnd),
                new InsertionPointProvider(), p.opts,
                rval -> {
                    Log.verbose(() -> "startFQL rq rval = " + rval);
                    if (rval != null) {
                        flag.val = true;
                        Node[] nodes = rval.getValue();
                        if (!p.rq.contains(nodes[0].key)) {
                            p.resultsReceiver.accept(null); // finish!
                            return;
                        }
                        forwardQueryLeft0(p, nodes[0], nodes[1], trace, visited);
                    } else {
                        if (!flag.val) {
                            System.err.println("forwardQueryLeft: couldn't find the start node");
                            p.resultsReceiver.accept(null);
                        }
                    }
                });
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
            return;
        }
        InvokeProviderRequest<T> ev = new InvokeProviderRequest<>(current, 
                expectedRight, p.provider, p.qid);
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
                        p.resultsReceiver.accept(rep.result);
                    }
                    if (visited.size() >= p.num || !p.rq.contains(rep.pred.key)) {
                        p.resultsReceiver.accept(null); // finish!
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

    <T> CompletableFuture<RemoteValue<T>> invokeProvider(
            RQValueProvider<T> received, LocalNode localNode, RQRange r,
            long qid) {
        // obtain the registered provider
        RQValueProvider<T> rprovider = getProvider(received.getClass());
        CompletableFuture<T> f;
        try {
            f = rprovider.getRaw(received, localNode, r, qid);
        } catch (Throwable exc) {
            // if getRaw terminates exceptionally...
            System.err.println("invokeProvider: got " + exc);
            exc.printStackTrace();
            RemoteValue<T> rval = new RemoteValue<>(getLocalNode().peerId, exc);
            return CompletableFuture.completedFuture(rval);
        }
        return f.handle((T val, Throwable exc) -> {
            RemoteValue<T> rval;
            if (exc != null) {
                rval = new RemoteValue<>(getLocalNode().peerId, exc);
            } else {
                rval = new RemoteValue<>(getLocalNode().peerId, val);
            }
            return rval;
        });
    }
    
    /*
     * classes for forwardQueryLeft
     */
    public static class InvokeProviderRequest<T>
    extends RequestEvent<InvokeProviderRequest<T>, InvokeProviderReply<T>> {
        final RQValueProvider<T> provider;
        final Node expectedSucc;
        final long qid;
        public InvokeProviderRequest(Node receiver, Node expectedSucc,
                RQValueProvider<T> provider, long qid) {
            super(receiver);
            this.expectedSucc = expectedSucc;
            this.provider = provider;
            this.qid = qid;
        }
        @Override
        public void run() {
            LocalNode local = getLocalNode();
            if (expectedSucc == null) {
                // special case
                Event ev = new InvokeProviderReply<>(this, null, true,
                        local.pred, local.succ);
                getLocalNode().post(ev);
                return;
            }
            if (expectedSucc == getLocalNode().succ) {
                RQStrategy strategy = RQStrategy.getRQStrategy(getLocalNode());
                CompletableFuture<RemoteValue<T>> f
                    = strategy.invokeProvider(provider, getLocalNode(), null, qid);
                f.thenAccept(rval -> {
                    InvokeProviderReply<T> ev = new InvokeProviderReply<>(this,
                            rval, true, local.pred, local.succ);
                    getLocalNode().post(ev);
                });
            } else {
                Event ev = new InvokeProviderReply<>(this, null, false,
                        local.pred, local.succ);
                getLocalNode().post(ev);
            }
        }
    }
    
    public static class InvokeProviderReply<T>
    extends ReplyEvent<InvokeProviderRequest<T>, InvokeProviderReply<T>> {
        final Node pred;
        final Node succ;
        final RemoteValue<T> result;
        final boolean success;
        public InvokeProviderReply(InvokeProviderRequest<T> req,
                RemoteValue<T> result, boolean success, Node pred, Node succ) {
            super(req);
            this.pred = pred;
            this.succ = succ;
            this.result = result;
            this.success = success;
        }
    }
}
