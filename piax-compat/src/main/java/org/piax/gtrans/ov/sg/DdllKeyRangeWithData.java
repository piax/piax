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

import org.piax.common.DdllKey;
import org.piax.common.subspace.Range;

/**
 * A range of DdllKey with a supplementary data (aux). 
 *  
 * @author k-abe
 * @param <E> type of the supplementary data
 */
public class DdllKeyRangeWithData<E> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /**
     * Split a range with given values, taken from the keys of `ents'. 
     *
     * @param <E>   the type of aux part
     * @param r     the range to be split
     * @param ents  the keys
     * @return  ranges split
     */
    public static <E> List<DdllKeyRangeWithData<E>> split(Range<DdllKey> r, 
            NavigableMap<DdllKey, E> ents) {
        List<DdllKeyRangeWithData<E>> ranges = new ArrayList<DdllKeyRangeWithData<E>>();
        E aux = null;
        if (ents.containsKey(r.from)) {
            aux = ents.get(r.from);
        }
        for (Map.Entry<DdllKey, E> ent: ents.entrySet()) {
            Range<DdllKey>[] split = r.split(ent.getKey());
            if (split.length == 2) {
                ranges.add(new DdllKeyRangeWithData<E>(aux, split[0]));
                aux = ent.getValue();
            }
            r = split[split.length - 1];
        }
        ranges.add(new DdllKeyRangeWithData<E>(aux, r));
        return ranges;
    }

    E aux;
    final Range<DdllKey> range;
    
    public DdllKeyRangeWithData(E aux, Range<DdllKey> range) {
        this.aux = aux;
        this.range = range;
    }

    public DdllKeyRangeWithData<E> concatenate(DdllKeyRangeWithData<E> another, boolean auxRight) {
        if (range.to.compareTo(another.range.from) != 0) {
            throw new IllegalArgumentException("not continuous: " + this
                    + " and " + another);
        }
        DdllKeyRangeWithData<E> kr = new DdllKeyRangeWithData<E>(auxRight ? another.aux : aux,
                new Range<DdllKey>(range.from, range.fromInclusive,
                        another.range.to, another.range.toInclusive));
        return kr;
    }
    
    @Override
    public String toString() {
        return range + "(" + aux + ")";
    }
}
