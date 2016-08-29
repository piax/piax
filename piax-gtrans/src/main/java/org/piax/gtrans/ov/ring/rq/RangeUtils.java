/*
 * RangeUtils.java - RangeUtils implementation of SkipGraph.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.piax.common.subspace.CircularRange;
import org.piax.common.subspace.Range;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Node;
import org.piax.util.KeyComparator;

/**
 * A utility class for range manipulations.
 * Note: 開区間(..)に対して厳密な考慮はされていない．
 */
public class RangeUtils {
    private static KeyComparator keyComp = KeyComparator.getInstance();

    /**
     * Range r から，[a, b) の区間を削除し，残った Range を返す．
     * aはrの外側にあるか，aはrの左端と等しい必要がある (手抜き)．
     * 残らない場合は null が返される．
     * 
     * r         [-------------------)
     *     [a--------------b)
     *                      [--ret---)
     *     
     * @param <K>
     * @param r
     * @param a
     * @param b
     * @return
     */
    public static <K extends Comparable<K>> CircularRange<K> retainRange(
            CircularRange<K> r, K a, K b) {
        if (r.contains(a) && keyComp.compare(a, r.from) != 0) {
            throw new Error("a is in r");
        }
        if (Node.isOrdered(a, r.from, b) && keyComp.compare(r.from, b) != 0) {
            // Range   [-----------)
            //      a-----..
            if (Node.isOrdered(a, r.to, b) && !r.contains(b)) {
                // empty
                return null;
            } else {
                // Range   [-----------)
                //     a------b
                //     -------b          a--
                return new CircularRange<K>(b, true, r.to, r.toInclusive);
            }
        } else {
            // Range      [------)
            //       a--b
            //       --b           a-
            return r;
        }
    }

    /**
     * Range r から，[a, b) の区間を削除したときの，削除された Range を返す．
     * aはrの外側にあるか，aはrの左端と等しい必要がある (手抜き)．
     * 削除されない場合は null が返る．
     * 
     * r         [-------------------)
     *     [a--------------b)
     *           [---ret----)
     *
     * @param <K>
     * @param r
     * @param a
     * @param b
     * @return
     */
    public static <K extends Comparable<K>> Range<K> removedRange(Range<K> r, K a, K b) {
        if (r.contains(a) && keyComp.compare(a, r.from) != 0) {
            throw new Error("a is in r");
        }
        if (Node.isOrdered(a, r.from, b) && keyComp.compare(r.from, b) != 0) {
            // Range   [-----------)
            //      a-----..
            if (Node.isOrdered(a, r.to, b) && !r.contains(b)) {
                // empty
                return r;
            } else {
                // Range   [-----------)
                //     a------b
                //     -------b          a--
                return new Range<K>(r.from, r.fromInclusive, b, true);
            }
        } else {
            // Range      [------)
            //       a--b
            //       --b           a-
            return null;
        }
    }

    public static List<CircularRange<DdllKey>> concatAdjacentRanges(
            List<CircularRange<DdllKey>> ranges) {
        Collections.sort(ranges, new Comparator<Range<?>>() {
            @Override
            public int compare(Range<?> o1, Range<?> o2) {
                return keyComp.compare(o1.from, o2.from);
            }
        });
        List<CircularRange<DdllKey>> merged =
                new ArrayList<CircularRange<DdllKey>>();
        CircularRange<DdllKey> prev = null;
        for (CircularRange<DdllKey> r : ranges) {
            if (prev == null) {
                prev = r;
            } else if (prev.to.compareTo(r.from) == 0) {
                prev =
                        new CircularRange<DdllKey>(prev.from,
                                prev.fromInclusive, r.to, r.toInclusive);
            } else {
                merged.add(prev);
                prev = r;
            }
        }
        if (prev != null) {
            merged.add(prev);
        }
        return merged;
    }

    public static <K extends Comparable<K>> boolean hasCommon(Range<K> range,
            K from, K to) {
        return (Node.isOrdered(from, range.from, to) || Node.isOrdered(from,
                range.to, to));
    }
}
