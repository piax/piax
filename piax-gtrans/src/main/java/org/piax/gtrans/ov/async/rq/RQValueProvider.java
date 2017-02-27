package org.piax.gtrans.ov.async.rq;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.piax.common.PeerId;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.async.EventExecutor;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.ov.ddll.DdllKey;

public abstract class RQValueProvider<T> implements Serializable {
    protected CompletableFuture<T> getRaw(LocalNode localNode, QueryId qid,
            DdllKey key) {
        return get(localNode, key);
    }

    public abstract CompletableFuture<T> get(LocalNode localNode, DdllKey key);

    public static class KeyProvider extends RQValueProvider<DdllKey> {
        @Override
        public CompletableFuture<DdllKey> get(LocalNode localNode,
                DdllKey key) {
            return CompletableFuture.completedFuture(key);
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

        @Override
        protected CompletableFuture<T> getRaw(LocalNode localNode, QueryId qid,
                DdllKey key) {
            RQStrategy s = RQStrategy.getRQStrategy(localNode);
            Map<PeerId, Map<QueryId, CompletableFuture<?>>> pmap =
                    s.resultCache;
            Map<QueryId, CompletableFuture<?>> qmap = pmap
                    .computeIfAbsent(localNode.peerId, k -> new HashMap<>());
            @SuppressWarnings("unchecked")
            CompletableFuture<T> f = (CompletableFuture<T>) qmap.get(qid);
            if (f == null) {
                f = get(localNode, key);
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
