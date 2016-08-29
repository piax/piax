/*
 * ChordSharpReplyMessage.java - A reply message of multi-key Chord##.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id$
 */
package org.piax.gtrans.ov.szk;

import java.util.Collection;

import org.piax.gtrans.ov.ring.RingManager;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;
import org.piax.gtrans.ov.ring.rq.MessagePath;
import org.piax.gtrans.ov.ring.rq.RQReplyMessage;

public class ChordSharpReplyMessage extends RQReplyMessage {
    private static final long serialVersionUID = 1L;

    public ChordSharpReplyMessage(RingManager<?> manager,
            ChordSharpRQMessage req,
            Collection<DKRangeRValue<?>> vals, boolean isFinal,
            Collection<MessagePath> paths, int hops) {
        super(manager, req, vals, isFinal, paths, hops, req.unavailableKeys);
    }

    @Override
    public String toString() {
        return String.format(
                "C#ReplyMessage(senderId=%s, replyId=%s, ackId=%s, vals=%s"
                        + ", isFinal=%s, N/A=%s, paths=%s)", senderId, replyId,
                ackId, vals, isFinal, unavailableKeys, paths);
    }
}
