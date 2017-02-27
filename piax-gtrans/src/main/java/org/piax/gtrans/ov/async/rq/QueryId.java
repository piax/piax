package org.piax.gtrans.ov.async.rq;

import org.piax.gtrans.async.Node;

class QueryId {
    Node origin;
    int rootEventId;
    public QueryId(RQRequest<?> req) {
        this.origin = req.origin;
        this.rootEventId = req.rootEventId;
    }
    @Override
    public String toString() {
        return "QID[" + rootEventId + ":" + origin.key + "]";
    }
    @Override
    public int hashCode() {
        return origin.hashCode() ^ rootEventId;
    }
    @Override
    public boolean equals(Object obj) {
        QueryId another = (QueryId)obj;
        return (origin == another.origin && rootEventId == another.rootEventId);
    }
}