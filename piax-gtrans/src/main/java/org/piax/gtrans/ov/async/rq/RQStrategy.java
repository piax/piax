package org.piax.gtrans.ov.async.rq;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.subspace.Range;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.async.Event.LocalEvent;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Node;
import org.piax.gtrans.async.NodeFactory;
import org.piax.gtrans.async.NodeStrategy;
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
        public LocalNode createNode(TransportId transId,
                ChannelTransport<?> trans, DdllKey key)
                throws IOException, IdConflictException {
            LocalNode node = base.createNode(transId, trans, key);
            node.pushStrategy(new RQStrategy());
            return node;
        }
        @Override
        public String name() {
            return "RQ/" + base.name();
        }
    }

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
    

    /**
     * ftlist (非故障ノードリスト) から，指定されたkeyのclosest predecessorを求める．
     * ただし，closest predecesorが自ノードになる場合で，自ノードのsuccessorの方が
     * keyに近い場合(=ftlistにsuccessorが含まれない場合)，successorを返す．
     * 
     * @param key the key
     * @param goodNodes the good nodes
     * @param allNodes the all nodes.
     * @param maybeFailed the nodes that may be failed.
     * @return the closest predecessor.
     */
    /*protected Node getClosestPredecessor(DdllKey key, List<Node> actives) {
        Node best = getClosestPredecessor0(key, actives);
        if (best.key.getUniqId().equals(n.getPeerId())) {
            Node best2 = getClosestPredecessor0(key, allNodes, null);
            logger.debug("getClosestPredecessor: case1: key={}, return {}",
                    key, best2);
            return best2;
        }
        logger.debug("getClosestPredecessor: case2: key={}, return {}", key,
                best);
        return best;
    }*/

    protected Node getClosestPredecessor(DdllKey key, List<Node> actives) {
        return actives.stream()
                .max(LocalNode.getComparator(key))
                .orElse(null);
    }
}