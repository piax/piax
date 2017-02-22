package org.piax.gtrans.ov.async.rq;

import java.util.Collection;

import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;

public class RQReplyDirect<T> extends Event {
    final Collection<DKRangeRValue<T>> vals;
    final int rootEventId;
    
    public RQReplyDirect(RQRequest<T> req, Collection<DKRangeRValue<T>> vals) {
        super(req.root);
        this.rootEventId = req.rootEventId;
        this.vals = vals;
    }

    @Override
    public void run() {
        LocalNode local = getLocalNode();
        @SuppressWarnings("unchecked")
        RQRequest<T> ev = (RQRequest<T>) RequestEvent.lookupRequestEvent(local,
                rootEventId);
        if (ev == null) {
            System.out.println("No RQRequest found: " + rootEventId);
        } else {
            ev.receiveReply(this);
        }
    }
    
    @Override
    public String toString() {
        return "RQReplyDirect[ID=" + getEventId()
        + ", rootEvId=" + rootEventId
        + ", vals="
        + (vals.size() > 10 ? "(" + vals.size() + " entries)" : vals)
        + "]";
    }
}