/*
 * RQEvent.java - An event for Range Query
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.ayame.ov.rq;

import java.util.concurrent.CompletableFuture;

import org.piax.ayame.Event;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;
import org.piax.ayame.Event.ReplyEvent;
import org.piax.ayame.Event.RequestEvent;
import org.piax.gtrans.RemoteValue;

public abstract class RQEvent {
    /*
     * classes for forwardQueryLeft
     */
    public static class GetLocalValueRequest<T>
    extends RequestEvent<GetLocalValueRequest<T>, GetLocalValueReply<T>> {
        final RQAdapter<T> adapter;
        final Node expectedSucc;
        final long qid;
        public GetLocalValueRequest(Node receiver, Node expectedSucc,
                RQAdapter<T> adapter, long qid) {
            super(receiver);
            this.expectedSucc = expectedSucc;
            this.adapter = adapter;
            this.qid = qid;
        }
        @Override
        public void run() {
            LocalNode local = getLocalNode();
            if (expectedSucc == null) {
                // special case
                Event ev = new GetLocalValueReply<>(this, null, true,
                        local.pred, local.succ);
                local.post(ev);
                return;
            }
            if (expectedSucc == local.succ) {
                RQStrategy strategy = RQStrategy.getRQStrategy(local);
                CompletableFuture<RemoteValue<T>> f
                    = strategy.getLocalValue(adapter, local, null, qid);
                f.thenAccept(rval -> {
                    GetLocalValueReply<T> ev = new GetLocalValueReply<>(this,
                            rval, true, local.pred, local.succ);
                    local.post(ev);
                });
            } else {
                Event ev = new GetLocalValueReply<>(this, null, false,
                        local.pred, local.succ);
                local.post(ev);
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
