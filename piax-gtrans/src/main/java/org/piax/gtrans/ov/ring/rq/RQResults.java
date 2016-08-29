/*
 * RQResults.java - An object to hold range query results.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.ring.rq;

import java.util.Collection;

import org.piax.gtrans.FutureQueue;

/**
 * a class for accessing the details of a range query result.
 * See {@link RQManager#scalableRangeQueryPro(Collection, Object, int, boolean)}
 * 
 * @param <V>
 */
public class RQResults<V> {
    protected final RQReturn rqRet;

    public RQResults(RQReturn rqRet) {
        this.rqRet = rqRet;
    }

    public RQResults() {
        this.rqRet = null;
    }

    @SuppressWarnings("unchecked")
    public FutureQueue<V> getFutureQueue() {
        if (rqRet == null) {
            return FutureQueue.emptyQueue();
        }
        return (FutureQueue<V>) rqRet.getFutureQueue();
    }

    /**
     * returns message paths from the root node to leaf nodes.
     * 
     * @return message paths
     */
    public Collection<MessagePath> getMessagePaths() {
        return rqRet.getMessagePaths();
    }

    /**
     * returns max path length, which equals to the max hops from the root
     * node to leaf nodes.
     * 
     * @return max path length
     */
    public int getMaxPathLength() {
        int max = 0;
        for (MessagePath mp: getMessagePaths()) {
            max = Math.max(max, mp.getHopCount());
        }
        return max;
    }

    /**
     * returns number of messages for scattering the query message.
     * 
     * @return number of messages
     */
    public int getMessageCount() {
        return rqRet.getMessagePaths().size();
    }
}
