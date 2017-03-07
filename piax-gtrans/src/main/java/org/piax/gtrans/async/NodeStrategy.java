package org.piax.gtrans.async;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.ov.async.rq.RQValueProvider;
import org.piax.gtrans.ov.ddll.DdllKey;

public abstract class NodeStrategy {
    protected LocalNode n;
    int level;

    public void activate(LocalNode node) {
        this.n = node;
    }

    public Node getPredecessor() {
        return n.pred;
    }

    public Node getSuccessor() {
        return n.succ;
    }

    public LocalNode getLocalNode() {
        return n;
    }

    public List<List<Node>> getRoutingEntries() {
        return getLower().getRoutingEntries();
    }

    public boolean isResponsible(DdllKey key) {
        return Node.isIn2(key, n.key, getSuccessor().key);
    }

    public String toStringDetail() {
        return getLower().toStringDetail();
    }

    public void initInitialNode() {
        getLower().initInitialNode();
    }

    public void join(LookupDone lookupDone,
            CompletableFuture<Boolean> joinFuture) {
        getLower().join(lookupDone, joinFuture);
    }

    public void leave(CompletableFuture<Boolean> leaveComplete) {
        getLower().leave(leaveComplete);
    }

    public <T> void rangeQuery(Collection<? extends Range<?>> ranges,
            RQValueProvider<T> provider, TransOptions opts,
            Consumer<RemoteValue<T>> resultsReceiver) {
        getLower().rangeQuery(ranges, provider, opts, resultsReceiver);
    }

    public void handleLookup(Lookup lookup) {
        getLower().handleLookup(lookup);
    }
    
    public void foundMaybeFailedNode(Node node) {
        getLower().foundMaybeFailedNode(node);
    }

    public int getMessages4Join() {
        return getLower().getMessages4Join();
    }

    protected NodeStrategy getLower() {
        NodeStrategy lower = n.getLowerStrategy(this);
        return lower;
    }
}
