package org.piax.gtrans.ov.async.rq;

import java.util.concurrent.CompletableFuture;

import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.async.Event;
import org.piax.gtrans.async.Event.ReplyEvent;
import org.piax.gtrans.async.Event.RequestEvent;
import org.piax.gtrans.async.LocalNode;
import org.piax.gtrans.async.Node;

public abstract class RQEvent {
    /*
     * classes for forwardQueryLeft
     */
    public static class GetLocalValueRequest<T>
    extends RequestEvent<GetLocalValueRequest<T>, GetLocalValueReply<T>> {
        final RQFlavor<T> flavor;
        final Node expectedSucc;
        final long qid;
        public GetLocalValueRequest(Node receiver, Node expectedSucc,
                RQFlavor<T> flavor, long qid) {
            super(receiver);
            this.expectedSucc = expectedSucc;
            this.flavor = flavor;
            this.qid = qid;
        }
        @Override
        public void run() {
            LocalNode local = getLocalNode();
            if (expectedSucc == null) {
                // special case
                Event ev = new GetLocalValueReply<>(this, null, true,
                        local.pred, local.succ);
                getLocalNode().post(ev);
                return;
            }
            if (expectedSucc == getLocalNode().succ) {
                RQStrategy strategy = RQStrategy.getRQStrategy(getLocalNode());
                CompletableFuture<RemoteValue<T>> f
                    = strategy.getLocalValue(flavor, getLocalNode(), null, qid);
                f.thenAccept(rval -> {
                    GetLocalValueReply<T> ev = new GetLocalValueReply<>(this,
                            rval, true, local.pred, local.succ);
                    getLocalNode().post(ev);
                });
            } else {
                Event ev = new GetLocalValueReply<>(this, null, false,
                        local.pred, local.succ);
                getLocalNode().post(ev);
            }
        }
    }
    
    public static class GetLocalValueReply<T>
    extends ReplyEvent<GetLocalValueRequest<T>, GetLocalValueReply<T>> {
        final Node pred;
        final Node succ;
        final RemoteValue<T> result;
        final boolean success;
        public GetLocalValueReply(GetLocalValueRequest<T> req,
                RemoteValue<T> result, boolean success, Node pred, Node succ) {
            super(req);
            this.pred = pred;
            this.succ = succ;
            this.result = result;
            this.success = success;
        }
    }
}
