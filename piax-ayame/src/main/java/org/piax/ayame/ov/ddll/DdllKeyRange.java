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
import org.piax.common.subspace.CircularRange;
import org.piax.common.subspace.Range;

/**
 * A range of DdllKey
 */
public class DdllKeyRange extends CircularRange<DdllKey> {
    private static final long serialVersionUID = 1L;

    /*
     * Split a range with given values, taken from the keys of `ents'. 
     *
     * @param <E>   the type of aux part
     * @param r     the range to be split
     * @param ents  the keys
     * @return  ranges split
     */
    /*public static <E> List<DdllKeyRange> split(CircularRange<DdllKey> r,
            NavigableMap<DdllKey, E> ents) {
        List<DdllKeyRange> ranges = new ArrayList<DdllKeyRange>();
        for (Map.Entry<DdllKey, E> ent : ents.entrySet()) {
            CircularRange<DdllKey>[] split = r.split(ent.getKey());
            if (split.length == 2) {
                ranges.add(new DdllKeyRange(split[0]));
            }
            r = split[split.length - 1];
        }
        ranges.add(new DdllKeyRange(r));
        return ranges;
    }*/

    public DdllKeyRange(Range<DdllKey> range) {
        this(range.from, range.fromInclusive, range.to, range.toInclusive);
    }

    public DdllKeyRange(DdllKey from, boolean fromInclusive, DdllKey to,
            boolean toInclusive) {
        super(from, fromInclusive, to, toInclusive);
    }

    @Override
    public String toString() {
        return rangeString();
    }

    @Override
    public Range<DdllKey> newRange(DdllKey from, boolean fromInclusive,
            DdllKey to, boolean toInclusive) {
        return new DdllKeyRange(from, fromInclusive, to, toInclusive);
    }
}
