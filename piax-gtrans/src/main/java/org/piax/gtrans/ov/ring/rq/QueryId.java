/*
 * QueryId.java - A query identifier.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ring.rq;

import java.io.Serializable;

import org.piax.common.PeerId;

/**
 * QueryId is used by the range query algorithm to uniquely identify a query
 * message.
 */
@SuppressWarnings("serial")
public class QueryId implements Serializable {
    final PeerId sourcePeer;
    final long id;
//    public long timestamp; // filled in by each receiver peer

    public QueryId(PeerId sourcePeer, long id) {
        this.sourcePeer = sourcePeer;
        this.id = id;
    }

    @Override
    public boolean equals(Object obj) {
        QueryId o = (QueryId) obj;
        return sourcePeer.equals(o.sourcePeer) && id == o.id;
    }

    @Override
    public int hashCode() {
        return sourcePeer.hashCode() ^ (int) id;
    }

    @Override
    public String toString() {
        return "qid[" + sourcePeer + ":" + id + "]";
    }
}

