/*
 * DKRangeLink.java - A DDLLKeyRange link implementation.
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

import org.piax.common.subspace.Range;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;

/**
 * DdllKeyRange with a Link
 *
 */
public class DKRangeLink extends DdllKeyRange {
    final Link link;

    public DKRangeLink(Link link, DdllKey from, boolean fromInclusive,
            DdllKey to, boolean toInclusive) {
        super(from, fromInclusive, to, toInclusive);
        this.link = link;
    }

    public DKRangeLink(Link link, Range<DdllKey> range) {
        super(range);
        this.link = link;
    }

    public Link getLink() {
        return link;
    }
}
