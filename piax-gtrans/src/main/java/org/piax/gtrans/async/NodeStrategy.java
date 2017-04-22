package org.piax.gtrans.async;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.piax.common.subspace.Range;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.async.Event.Lookup;
import org.piax.gtrans.async.Event.LookupDone;
import org.piax.gtrans.ov.async.rq.RQAdapter;
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

    public List<FTEntry> getRoutingEntries() {
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
            RQAdapter<T> adapter, TransOptions opts) {
        System.out.println("lower=" + getLower());
        getLower().rangeQuery(ranges, adapter, opts);
    }

    public <T> void forwardQueryLeft(Range<?> range, int num,
            RQAdapter<T> adapter, TransOptions opts) {
        getLower().forwardQueryLeft(range, num, adapter, opts);
    }
    
    public void handleLookup(Lookup lookup) {
        getLower().handleLookup(lookup);
    }

    public void foundMaybeFailedNode(Node node) {
        getLower().foundMaybeFailedNode(node);
    }

    /**
     * get a (cloned) FTEntry for sending to a remote node.
     * 
     * @param fromDist
     * @param toDist
     * @return a FTEntry
     */
    public FTEntry getFTEntryToSend(int fromDist, int toDist) {
        return getLower().getFTEntryToSend(fromDist, toDist);
    }

    // tentative solution
    public FTEntry getFingerTableEntry(boolean isBackward, int index) {
        return getLower().getFingerTableEntry(isBackward, index);
    }
    
    // tentative solution
    public Object getLocalCollectedData(Class<? extends RQAdapter<?>> clazz) {
        return getLower().getLocalCollectedData(clazz);
    }

    public int getMessages4Join() {
        return getLower().getMessages4Join();
    }

    protected NodeStrategy getLower() {
        NodeStrategy lower = n.getLowerStrategy(this);
        return lower;
    }
}
