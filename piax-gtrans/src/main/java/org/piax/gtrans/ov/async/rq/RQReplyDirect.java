package org.piax.gtrans.ov.async.rq;

import java.util.Collection;

import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;

public class RQReplyDirect extends Event {
    final Collection<DKRangeRValue<?>> vals;
    final int rootEventId;
    
    public RQReplyDirect(RQRequest req, Collection<DKRangeRValue<?>> vals) {
        super(req.root);
        this.rootEventId = req.rootEventId;
        this.vals = vals;
    }

    @Override
    public void run() {
        LocalNode local = getLocalNode();
        RQRequest ev = (RQRequest) RequestEvent.lookupRequestEvent(local,
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