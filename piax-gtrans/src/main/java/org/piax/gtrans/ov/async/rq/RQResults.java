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
package org.piax.gtrans.ov.async.rq;

import java.util.Collection;

import org.piax.gtrans.TransOptions;
import org.piax.gtrans.ov.async.rq.RQRequest.RQCatcher;
import org.piax.gtrans.ov.ring.rq.RQManager;

/**
 * a class for accessing the details of a range query result.
 * See {@link RQManager#scalableRangeQueryPro(Collection, Object, TransOptions)}
 * 
 * @param <V> a type for range query results.
 */
public class RQResults<V> {
    protected final RQCatcher catcher;

    public RQResults(RQCatcher catcher) {
        this.catcher = catcher;
    }
/*
    public RQResults() {
        this.catcher = null;
    }

    @SuppressWarnings("unchecked")
    public FutureQueue<V> getFutureQueue() {
        if (catcher == null) {
            return FutureQueue.emptyQueue();
        }
        return (FutureQueue<V>) catcher.getFutureQueue();
    }*/
}
