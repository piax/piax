package org.piax.ayame.ov.rq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.piax.ayame.Event;
import org.piax.ayame.LocalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RQReplyDirect<T> extends Event {
    private static final Logger logger = LoggerFactory.getLogger(RQReplyDirect.class);
    final Collection<DKRangeRValue<T>> vals;
    final int rootEventId;
    
    public RQReplyDirect(RQRequest<T> req, Collection<DKRangeRValue<T>> vals) {
        super(req.root);
        this.rootEventId = req.rootEventId;
        if (vals == null || vals instanceof Serializable) {
            this.vals = vals;
        } else {
            this.vals = new ArrayList<>(vals); 
        }
    }

    @Override
    public void run() {
        LocalNode local = getLocalNode();
        @SuppressWarnings("unchecked")
        RQRequest<T> ev = (RQRequest<T>) RequestEvent.lookupRequestEvent(local,
                rootEventId);
        if (ev == null) {
            logger.debug("No RQRequest found: {}", rootEventId);
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