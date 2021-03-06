/*
 * NodeStrategy.java - An abstract class of node strategy
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.piax.ayame.Event.Lookup;
import org.piax.ayame.Event.LookupDone;
import org.piax.ayame.ov.rq.RQAdapter;
import org.piax.common.DdllKey;
import org.piax.common.subspace.Range;
import org.piax.gtrans.TransOptions;

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

    public CompletableFuture<Void> join(LookupDone lookupDone) {
        return getLower().join(lookupDone);
    }

    public CompletableFuture<Void> leave() {
        return getLower().leave();
    }

    public <T> void rangeQuery(Collection<? extends Range<?>> ranges,
            RQAdapter<T> adapter, TransOptions opts) {
        getLower().rangeQuery(ranges, adapter, opts);
    }

    public <T> void forwardQueryLeft(Range<?> range, int num,
            RQAdapter<T> adapter, TransOptions opts) {
        getLower().forwardQueryLeft(range, num, adapter, opts);
    }

    public void handleLookup(Lookup lookup) {
        getLower().handleLookup(lookup);
    }

    /**
     * get a (cloned) FTEntry for sending to a remote node.
     * 
     * @param fromDist distance to the start node (inclusive)
     * @param toDist   distance to the end index (exclusive)
     * @return a FTEntry for sending to a remote node
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

    protected NodeStrategy getLower() {
        NodeStrategy lower = n.getLowerStrategy(this);
        return lower;
    }
}
