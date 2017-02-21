package org.piax.gtrans.ov.async.rq;

import java.util.Collection;

import org.piax.gtrans.async.Event.ReplyEvent;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;

public class RQReply extends ReplyEvent<RQRequest, RQReply> {
    protected final Collection<DKRangeRValue<?>> vals;
    /** is final reply? */
    protected final boolean isFinal;
    
    public RQReply(RQRequest req, Collection<DKRangeRValue<?>> vals,
            boolean isFinal) {
        super(req);
        this.vals = vals;
        this.isFinal = isFinal;
    }
    
    @Override
    public String toStringMessage() {
        return "RQReply["
                + String.join(", ", stringify(vals), bool(isFinal, "isFinal"))
                + "]";
    }
    
    protected String stringify(Object o) {
        if (o == null) {
            return "null";
        } else {
            return o.toString();
        }
    }
    protected String bool(boolean val, String name) {
        if (val) {
            return name;
        } else {
            return "";
        }
    }
}