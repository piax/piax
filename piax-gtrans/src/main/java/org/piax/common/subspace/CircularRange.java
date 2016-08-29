/*
 * CircularRange.java - A range that allows wrap-around.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: VarDestinationPair.java 718 2013-07-07 23:49:08Z yos $
 */
package org.piax.common.subspace;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.piax.gtrans.ov.ddll.Node;

/**
 * a range that allows wrap-around.
 */
public class CircularRange<K extends Comparable<?>> extends Range<K> {
    public CircularRange(K from, K to) {
        this(from, true, to, true);
    }

    public CircularRange(K key) {
        this(key, true, key, true);
    }

    public CircularRange(K from, boolean fromInclusive, K to,
            boolean toInclusive) {
        super(false, from, fromInclusive, to, toInclusive);
    }

    protected CircularRange<K> newInstance(K from, boolean fromInclusive, K to,
            boolean toInclusive) {
        return new CircularRange<K>(from, fromInclusive, to, toInclusive);
    }

    @SuppressWarnings("unchecked")
    public CircularRange<K>[] split(K k) {
        if (keyComp.compare(from, k) < 0 && keyComp.compare(k, to) < 0) {
            CircularRange<K> left = newInstance(from, fromInclusive, k, false);
            CircularRange<K> right = newInstance(k, true, to, toInclusive);
            return new CircularRange[] { left, right };
        } else {
            return new CircularRange[] { this };
        }
    }

    /**
     * このインスタンスが表す範囲からrを削除した場合に残る範囲のリストを返す．
     * 削除した範囲のリストは intersect に追加する．
     *
     * <pre>
     * example:
     *   this:     [==========]
     *   r:           [===]
     *   returned: [=]     [==]
     *
     *   this:     [==========]
     *   r:                 [===]
     *   returned: [=======]
     *
     * </pre>
     * @param r         range to delete
     * @param intersect the list to add ranges that intersects with r
     * @return a list of retained ranges.
     */
    public List<CircularRange<K>> retain(Range<K> r,
            List<? super Range<K>> intersect) {
        if (r.contains(this)) {
            if (intersect != null) {
                intersect.add(this);
            }
            return null; // nothing is left
        }
        if (isWhole()) {
            if (intersect != null) {
                intersect.add(r);
            }
            return Collections.singletonList(new CircularRange<K>(r.to,
                    !r.toInclusive, r.from, !r.fromInclusive));
        }
        if (this.contains(r.from) && !this.contains(r.to)) {
            // range: [ ...... ]
            // r:         [ .........]
            addIfValidRange(intersect, r.from, r.fromInclusive, this.to,
                    this.toInclusive);
            return Collections.singletonList(new CircularRange<K>(from,
                    fromInclusive, r.from, !r.fromInclusive));
        }
        if (!this.contains(r.from) && this.contains(r.to)) {
            // range:     [ ...... ]
            // r:  [ .........]
            addIfValidRange(intersect, this.from, this.fromInclusive, r.to,
                    r.toInclusive);
            return Collections.singletonList(new CircularRange<K>(r.to,
                    !r.toInclusive, to, toInclusive));
        }

        if (this.contains(r.from) && this.contains(r.to)) {
            if (Node.isOrdered(r.from, this.to, r.to) && !r.isSingleton()
                    && keyComp.compare(this.to, r.to) != 0) {
                List<CircularRange<K>> tmp = new ArrayList<CircularRange<K>>();
                /*
                 *   range:  [ ..... ]
                 *   r:    ....]   [.....
                 */
                addIfValidRange(tmp, this.from, this.fromInclusive, r.to,
                        r.toInclusive);
                addIfValidRange(tmp, r.from, r.fromInclusive, this.to,
                        this.toInclusive);
                merge2(tmp);
                if (intersect != null) {
                    intersect.addAll(tmp);
                }
                return Collections.singletonList(new CircularRange<K>(r.to,
                        !r.toInclusive, r.from, !r.fromInclusive));
            }
            /*
             *   range:  [ ..... ]
             *   r:        [...]
             */
            if (intersect != null) {
                intersect.add(r);
            }
            List<CircularRange<K>> retain = new ArrayList<CircularRange<K>>();
            if (keyComp.compare(from, r.from) != 0) {
                CircularRange<K> r1 =
                        new CircularRange<K>(from, fromInclusive, r.from,
                                !r.fromInclusive);
                retain.add(r1);
            }
            if (keyComp.compare(r.to, to) != 0) {
                CircularRange<K> r2 =
                        new CircularRange<K>(r.to, !r.toInclusive, to,
                                toInclusive);
                retain.add(r2);
            }
            merge2(retain);
            return retain;
        }
        return Collections.singletonList(this);
    }

    private void addIfValidRange(List<? super CircularRange<K>> intersect,
            K from, boolean fromInclusive, K to, boolean toInclusive) {
        if (intersect == null) {
            return;
        }
        boolean valid =
                (keyComp.compare(from, to) != 0 || (fromInclusive && toInclusive));
        if (valid) {
            intersect.add(new CircularRange<K>(from, fromInclusive, to,
                    toInclusive));
        }
    }

    public boolean isFollowedBy(CircularRange<K> another) {
        return (keyComp.compare(this.to, another.from) == 0
                && (this.toInclusive ^ another.fromInclusive));
    }

    public CircularRange<K> concatenate(CircularRange<K> another) {
        if (isFollowedBy(another)) {
            CircularRange<K> r =
                    new CircularRange<K>(this.from, this.fromInclusive,
                            another.to, another.toInclusive);
            return r;
        }
        throw new IllegalArgumentException(this + " and " + another
                + "is not adjcent");
    }

    private static <K extends Comparable<?>> void merge2(
            List<CircularRange<K>> ranges) {
        if (ranges.size() <= 1) {
            return;
        }
        assert ranges.size() == 2;
        CircularRange<K> r1 = ranges.get(0);
        CircularRange<K> r2 = ranges.get(1);
        if (r2.isFollowedBy(r1)) {
            CircularRange<K> r = r2.concatenate(r1);
            ranges.clear();
            ranges.add(r);
        }
    }
}
