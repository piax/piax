package org.piax.gtrans.ov.async.rq;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.piax.common.PeerId;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Log;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.ov.async.rq.RQRequest.SPECIAL;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;
import org.piax.gtrans.ov.ring.rq.DdllKeyRange;

public abstract class RQValueProvider<T> implements Serializable {
    /**
     * @param localNode   the node that receives the request
     * @param range       the range that should be handled by this node
     * @param qid         query ID
     * @param key         the key that corresponds to localNode
     * @return
     */
    @SuppressWarnings("unchecked")
    protected CompletableFuture<T> getRaw(RQValueProvider<T> received,
            LocalNode localNode, DdllKeyRange range, long qid) {
        if (range != null && !range.contains(localNode.key)) {
            // although SPECIAL.PADDING is not a type of T, using
            // it as T is safe because it is used just as a marker. 
            return CompletableFuture.completedFuture((T) SPECIAL.PADDING);
        } else {
            return get(received, localNode.key);
        }
    }

    public abstract CompletableFuture<T> get(RQValueProvider<T> received,
            DdllKey key);
    
    public boolean select(RQRange range, T data) {
        return true; // select all by default
    }
    
    public boolean doReduce() {
        return false;
    }

    public T reduce(T a, T b) {
        return null; // don't reduce by default
    }

    public static class KeyProvider extends RQValueProvider<DdllKey> {
        @Override
        public CompletableFuture<DdllKey> get(RQValueProvider<DdllKey> received,
                DdllKey key) {
            return CompletableFuture.completedFuture(key);
        }
    }

    public static class InsertionPointProvider extends RQValueProvider<Node[]> {
        @Override
        protected CompletableFuture<Node[]> getRaw(RQValueProvider<Node[]> received,
                LocalNode localNode, DdllKeyRange range, long qid) {
            Node[] ret = new Node[] { localNode, localNode.succ };
            return CompletableFuture.completedFuture(ret);
        }

        @Override
        public CompletableFuture<Node[]> get(RQValueProvider<Node[]> received,
                DdllKey key) {
            return null; // dummy
        }
    }

    /**
     * a result value provider that caches the result. 
     * use this class as a base class of value provider that has side-effects.
     * 
     * @see TransOptions.DeliveryMode
     *  
     * @param <T> type of the value
     */
    public abstract static class CacheProvider<T> extends RQValueProvider<T> {
        final long cachePeriod;

        public CacheProvider(long cachePeriod) {
            this.cachePeriod = cachePeriod;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected CompletableFuture<T> getRaw(RQValueProvider<T> received,
                LocalNode localNode, DdllKeyRange range, long qid) {
            if (!range.contains(localNode.key)) {
                return CompletableFuture.completedFuture((T) SPECIAL.PADDING);
            }
            RQStrategy s = RQStrategy.getRQStrategy(localNode);
            Map<PeerId, Map<Long, CompletableFuture<?>>> pmap = s.resultCache;
            Map<Long, CompletableFuture<?>> qmap = pmap
                    .computeIfAbsent(localNode.peerId, k -> new HashMap<>());
            Log.verbose(() -> "getRaw: qid=" + qid);
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
