package org.piax.gtrans.ov.async.rq;

import java.util.Collection;
import java.util.HashMap;
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
import org.piax.gtrans.async.Event.LocalEvent;
import org.piax.gtrans.async.LocalNode;
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
        
        n.post(new LocalEvent(n, () -> {
            RQRequest<T> root = new RQRequest<>(n, rqranges, provider, opts,
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

    protected Node getClosestPredecessor(DdllKey key, List<Node> actives) {
        return actives.stream()
                .max(LocalNode.getComparator(key))
                .orElse(null);
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
}
