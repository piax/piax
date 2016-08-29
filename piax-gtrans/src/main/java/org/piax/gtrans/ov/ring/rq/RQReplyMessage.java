/*
 * RQReplyMessage.java - A class of reply message for range queries.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.ring.rq;

import java.util.Collection;
import java.util.List;

import org.piax.common.PeerId;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ring.ReplyMessage;
import org.piax.gtrans.ov.ring.RingManager;

/**
 * a class representing a reply message against {@link RQMessage},
 * containing range query results.
 */
public class RQReplyMessage extends ReplyMessage {
    private static final long serialVersionUID = 1L;
    protected final PeerId senderId;
    protected final Collection<DKRangeRValue<?>> vals;
    /** is final reply? */
    protected final boolean isFinal;
    protected final Collection<MessagePath> paths;
    protected final int hops;

    /**
     * constructor.
     * 
     * @param manager ring manager
     * @param req the RQMessage that this reply corresponds to.
     * @param vals return values
     * @param isFinal true if this reply message is the final message and no
     *            more reply message will be sent
     * @param paths message delivery paths
     * @param hops max hop count observed by this node (maybe)
     */
    public RQReplyMessage(RingManager<?> manager, RQMessage req,
            Collection<DKRangeRValue<?>> vals, boolean isFinal,
            Collection<MessagePath> paths, int hops,
            List<DdllKey> unavailableKeys) {
        super(manager, req, unavailableKeys);
        this.senderId = manager.getPeerId();
        this.vals = vals;
        this.isFinal = isFinal;
        this.paths = paths;
        this.hops = hops;
    }

    @Override
    public String toString() {
        return String
                .format("RQReplyMessage(senderId=%s, replyId=%s, vals=%s, isFinal=%s, paths=%s)",
                        senderId, replyId, vals, isFinal, paths);
    }
}