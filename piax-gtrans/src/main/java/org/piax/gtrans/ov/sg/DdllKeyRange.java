/*
 * DdllKeyRange.java - DdllKeyRange implementation of SkipGraph.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: DdllKeyRange.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.ov.sg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.piax.common.subspace.Range;
import org.piax.gtrans.ov.ddll.DdllKey;

/**
 * A range of DdllKey with a supplementary data (aux). 
 *  
 * @author k-abe
 * @param <E> type of the supplementary data
 */
public class DdllKeyRange<E> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /**
     * Split a range with given values, taken from the keys of `ents'. 
     *
     * @param <E>   the type of aux part
     * @param r     the range to be split
     * @param ents  the keys
     * @return  ranges split
     */
    public static <E> List<DdllKeyRange<E>> split(Range<DdllKey> r, 
            NavigableMap<DdllKey, E> ents) {
        List<DdllKeyRange<E>> ranges = new ArrayList<DdllKeyRange<E>>();
        E aux = null;
        if (ents.containsKey(r.from)) {
            aux = ents.get(r.from);
        }
        for (Map.Entry<DdllKey, E> ent: ents.entrySet()) {
            Range<DdllKey>[] split = r.split(ent.getKey());
            if (split.length == 2) {
                ranges.add(new DdllKeyRange<E>(aux, split[0]));
                aux = ent.getValue();
            }
            r = split[split.length - 1];
        }
        ranges.add(new DdllKeyRange<E>(aux, r));
        return ranges;
    }

    E aux;
    final Range<DdllKey> range;
    
    public DdllKeyRange(E aux, Range<DdllKey> range) {
        this.aux = aux;
        this.range = range;
    }

    public DdllKeyRange<E> concatenate(DdllKeyRange<E> another, boolean auxRight) {
        if (range.to.compareTo(another.range.from) != 0) {
            throw new IllegalArgumentException("not continuous: " + this
                    + " and " + another);
        }
        DdllKeyRange<E> kr = new DdllKeyRange<E>(auxRight ? another.aux : aux,
                new Range<DdllKey>(range.from, range.fromInclusive,
                        another.range.to, another.range.toInclusive));
        return kr;
    }
    
    @Override
    public String toString() {
        return range + "(" + aux + ")";
    }
}
