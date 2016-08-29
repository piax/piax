/*
 * MessagePath.java - An object to retain message path. 
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

import java.io.Serializable;
import java.util.Collection;

import org.piax.gtrans.ov.ddll.DdllKey;

public class MessagePath implements Serializable {
    private static final long serialVersionUID = 1L;
    final int hop;
    final DdllKey from;
    final DdllKey to;
    final Collection<? extends DKRangeLink> targetRanges;

    public MessagePath(int hop, DdllKey from, DdllKey to,
            Collection<? extends DKRangeLink> targetRanges) {
        this.hop = hop;
        this.from = from;
        this.to = to;
        this.targetRanges = targetRanges;
    }

    @Override
    public String toString() {
        return String.format("path(hop=%d, %s to %s, target=%s)", hop,
                from, to, targetRanges);
    }

    public int getHopCount() {
        return hop;
    }

    public DdllKey getFrom() {
        return from;
    }

    public DdllKey getTo() {
        return to;
    }

    public Collection<? extends DKRangeLink> getTargetRanges() {
        return targetRanges;
    }
}
