/*
 * DdllKeyRange.java - A DdllKeyRange implementation.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */

package org.piax.ayame.ov.ddll;

import org.piax.common.DdllKey;
import org.piax.common.subspace.Range;

/**
 * A range of DdllKey
 */
public class DdllKeyRange extends Range<DdllKey> {
    private static final long serialVersionUID = 1L;

    public DdllKeyRange(Range<DdllKey> range) {
        this(range.from, range.fromInclusive, range.to, range.toInclusive);
    }

    public DdllKeyRange(DdllKey from, boolean fromInclusive, DdllKey to,
            boolean toInclusive) {
        super(true, from, fromInclusive, to, toInclusive);
    }

    @Override
    public String toString() {
        return rangeString();
    }

    @Override
    public DdllKeyRange newRange(DdllKey from, boolean fromInclusive,
            DdllKey to, boolean toInclusive) {
        return new DdllKeyRange(from, fromInclusive, to, toInclusive);
    }
}
