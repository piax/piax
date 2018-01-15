package org.piax.ayame.ov.rq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.piax.ayame.Event.ReplyEvent;

public class RQReply<T> extends ReplyEvent<RQRequest<T>, RQReply<T>> {
    protected final Collection<DKRangeRValue<T>> vals;
    /** is final reply? */
    protected final boolean isFinal;
    
    public RQReply(RQRequest<T> req, Collection<DKRangeRValue<T>> vals,
            boolean isFinal) {
        super(req);
        if (vals == null || vals instanceof Serializable) {
            this.vals = vals;
        } else {
            this.vals = new ArrayList<>(vals); 
        }
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
            return "!" + name;
        }
    }
}