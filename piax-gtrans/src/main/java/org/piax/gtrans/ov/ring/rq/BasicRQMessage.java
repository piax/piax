/*
 * BasicRQMessage.java - A basic message for range query overlay.
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

import java.util.Collection;

import org.piax.common.Endpoint;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.ov.ring.MessagingFramework;
import org.piax.gtrans.ov.ring.RingManager;

public class BasicRQMessage extends RQMessage {
    private static final long serialVersionUID = 1L;

    /**
     * create an instance of RQMessage used for a root node.
     * <p>
     * this instance is used only for creating child RQMessage and never receive
     * any reply message.
     * 
     * @param msgframe
     * @param subRanges
     * @param qid
     * @param query
     * @param expire
     * @param scalableReturn
     * @param opts
     * @return an instance of RQMessage
     */
    public static RQMessage newRQMessage4Root(MessagingFramework msgframe,
            Collection<SubRange> subRanges, QueryId qid, Object query,
            TransOptions opts) {
        RQMessage msg =
                new BasicRQMessage(msgframe, true,
                        TransOptions.responseType(opts) == ResponseType.DIRECT
                                ? msgframe.getEndpoint() : null,
                        MessagingFramework.DUMMY_MSGID, subRanges, qid, query,
                        0, opts);
        return msg;
    }

    protected BasicRQMessage(MessagingFramework sgmf, boolean isRoot,
            Endpoint replyTo, int replyId, Collection<SubRange> subRanges,
            QueryId qid, Object query, int hops, TransOptions opts) {
        super(sgmf, isRoot, replyTo, replyId, subRanges, qid, query, hops, opts);
    }

    @Override
    public RQAlgorithm getRangeQueryAlgorithm() {
        return null; // BasicRQAlgorithm is abstract class
    }

    @Override
    protected BasicRQMessage createInstance(MessagingFramework sgmf,
            boolean isRoot, Endpoint replyTo, int replyId,
            Collection<SubRange> subRanges, TransOptions opts) {
        BasicRQMessage msg =
                new BasicRQMessage(sgmf, isRoot, replyTo, replyId, subRanges,
                        this.qid, this.query, this.hops + 1, opts);
        return msg;
    }

    @Override
    public void execute(RingManager<?> sg) {
        ((RQManager) sg).rqDisseminate(this);
    }

    @Override
    public String shortName() {
        return "BasicRQ";
    }

    @Override
    public RQReplyMessage newRQReplyMessage(Collection<DKRangeRValue<?>> vals,
            boolean isFinal, Collection<MessagePath> paths, int hops) {
        return null; // XXX
    }
}
