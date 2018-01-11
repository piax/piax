package org.piax.ayame.ov.rq;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.piax.ayame.EventExecutor;
import org.piax.ayame.FTEntry;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;
import org.piax.ayame.ov.ddll.DdllKeyRange;
import org.piax.ayame.ov.rq.RQRequest.SPECIAL;
import org.piax.common.DdllKey;
import org.piax.common.PeerId;
import org.piax.gtrans.RemoteValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * abstract base class of range query adapter.
 *
 * @param <T> type of the return value of the range query
 */
public abstract class RQAdapter<T> implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(RQAdapter.class);
    transient protected final Consumer<RemoteValue<T>> resultsReceiver;
    public RQAdapter(Consumer<RemoteValue<T>> resultsReceiver) {
        this.resultsReceiver = resultsReceiver;
    }

    public Class<? extends RQAdapter<T>> getClazz() {
        @SuppressWarnings("unchecked")
        Class<? extends RQAdapter<T>> clazz
                = (Class<? extends RQAdapter<T>>)getClass();
        return clazz;
    }
    /**
     * @param received    the RQAdapter that is received on this node
     * @param localNode   the node that receives the request
     * @param range       the range that should be handled by this node
     * @param qid         query ID
     * @return a CompletableFuture that provides the value
     */
    @SuppressWarnings("unchecked")
    protected CompletableFuture<T> getRaw(RQAdapter<T> received,
            LocalNode localNode, DdllKeyRange range, long qid) {
        if (range != null && !range.contains(localNode.key)) {
            // although SPECIAL.PADDING is not a type of T, using
            // it as T is safe because it is used just as a marker. 
            return CompletableFuture.completedFuture((T) SPECIAL.PADDING);
        } else {
            return get(received, localNode.key);
        }
    }

    public abstract CompletableFuture<T> get(RQAdapter<T> received,
            DdllKey key);

    /**
     * modify the behavior of <code>RQRequest#rqDisseminate()</code>
     * 
     * @param queryRanges list of query range
     * @param ftents list of finger table entry
     * @param locallyResolved an initially empty list to store ranges that
     *                        are not necessary to ask child nodes.
     * @return 子ノードに処理を委譲する範囲のリスト
     */
    public List<RQRange> preprocess(List<RQRange> queryRanges,
            List<FTEntry> ftents, List<DKRangeRValue<T>> locallyResolved) {
        return queryRanges;
    }

    public void handleResult(RemoteValue<T> result) {
        this.resultsReceiver.accept(result);
    }

    public Object getCollectedData(LocalNode localNode) {
        return null;
    }

    public Object reduceCollectedData(List<?> value) {
        return null;
    }

    public static class KeyAdapter extends RQAdapter<DdllKey> {
        public KeyAdapter(Consumer<RemoteValue<DdllKey>> resultsReceiver) {
            super(resultsReceiver);
        }
        @Override
        public CompletableFuture<DdllKey> get(RQAdapter<DdllKey> received,
                DdllKey key) {
            return CompletableFuture.completedFuture(key);
        }
    }

    public static class InsertionPointAdapter extends RQAdapter<Node[]> {
        public InsertionPointAdapter(Consumer<RemoteValue<Node[]>> resultsReceiver) {
            super(resultsReceiver);
        }
        @Override
        protected CompletableFuture<Node[]> getRaw(RQAdapter<Node[]> received,
                LocalNode localNode, DdllKeyRange range, long qid) {
            Node[] ret = new Node[] { localNode, localNode.succ };
            return CompletableFuture.completedFuture(ret);
        }

        @Override
        public CompletableFuture<Node[]> get(RQAdapter<Node[]> received,
                DdllKey key) {
            return null; // dummy
        }
    }

    /**
     * a result value provider that caches the result. 
     * use this class as a base class of value provider that has side-effects.
     * 
     * @param <T> type of the value
     * @see org.piax.gtrans.TransOptions.DeliveryMode
     *  
     */
    public abstract static class CacheAdapter<T> extends RQAdapter<T> {
        final long cachePeriod;

        public CacheAdapter(Consumer<RemoteValue<T>> resultsReceiver,
                long cachePeriod) {
            super(resultsReceiver);
            this.cachePeriod = cachePeriod;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected CompletableFuture<T> getRaw(RQAdapter<T> received,
                LocalNode localNode, DdllKeyRange range, long qid) {
            if (!range.contains(localNode.key)) {
                return CompletableFuture.completedFuture((T) SPECIAL.PADDING);
            }
            RQStrategy s = RQStrategy.getRQStrategy(localNode);
            Map<PeerId, Map<Long, CompletableFuture<?>>> pmap = s.resultCache;
            Map<Long, CompletableFuture<?>> qmap = pmap
                    .computeIfAbsent(localNode.peerId, k -> new HashMap<>());
            logger.trace("getRaw: qid={}", qid);
            CompletableFuture<T> f = (CompletableFuture<T>) qmap.get(qid);
            if (f == null) {
                f = get(received, localNode.key);
                qmap.put(qid, f);
                f.thenRun(() -> {
                    EventExecutor.sched(
                            "purge_rqresults-" + qid + "@" + localNode.key,
                            cachePeriod, () -> qmap.remove(qid));
                });
            }
            return f;
        }
    }
}
