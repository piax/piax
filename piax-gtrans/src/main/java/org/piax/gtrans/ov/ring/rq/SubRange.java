/*
 * SubRange.java - Sub-range of DDLL key.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.piax.common.subspace.CircularRange;
import org.piax.common.subspace.Range;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.Node;

public class SubRange extends DKRangeLink {
    private static final long serialVersionUID = 1L;
    public final static int MAXID = 100000;
    public Integer[] ids;

    public SubRange(DdllKey from, boolean fromInclusive, DdllKey to,
            boolean toInclusive) {
        this(null, from, fromInclusive, to, toInclusive);
    }

    public SubRange(Link aux, DdllKey from, boolean fromInclusive, DdllKey to,
            boolean toInclusive) {
        this(aux, from, fromInclusive, to, toInclusive, null);
    }

    public SubRange(Link aux, Range<DdllKey> subRange) {
        this(aux, subRange, null);
    }

    public SubRange(Link aux, Range<DdllKey> subRange, Integer[] ids) {
        super(aux, subRange);
        this.ids = ids;
    }

    public SubRange(Link aux, DdllKey from, boolean fromInclusive, DdllKey to,
            boolean toInclusive, Integer[] ids) {
        super(aux, from, fromInclusive, to, toInclusive);
        this.ids = ids;
    }

    @Override
    public String toString() {
        return rangeString() + (getLink() != null ? "&link=" + getLink() : "")
                + (ids != null ? "&ids=" + Arrays.toString(ids) : "");
    }

    @Override
    protected SubRange newInstance(DdllKey from, boolean fromInclusive,
            DdllKey to, boolean toInclusive) {
        return new SubRange(from, fromInclusive, to, toInclusive);
    }

    @Override
    public SubRange[] split(DdllKey k) {
        CircularRange<DdllKey>[] s = super.split(k);
        // Java cannot cast CircularRange[] into SubRange[] so..
        SubRange[] ret = new SubRange[s.length];
        System.arraycopy(s, 0, ret, 0, s.length);
        return ret;
    }

    public List<SubRange> split(NavigableMap<DdllKey, Link> ents) {
        SubRange r = this;
        List<SubRange> ranges = new ArrayList<SubRange>();
        Link aux = null;
        if (ents.containsKey(this.from)) {
            aux = ents.get(this.from);
        }
        for (Map.Entry<DdllKey, Link> ent : ents.entrySet()) {
            SubRange[] split = this.split(ent.getKey());
            if (split.length == 2) {
                ranges.add(new SubRange(aux, split[0]));
                aux = ent.getValue();
            }
            r = split[split.length - 1];
        }
        ranges.add(new SubRange(aux, r));
        return ranges;
    }

    /*
    public <K extends Comparable<?>> SubRange retainRange(DdllKey a, DdllKey b) {
        if (this.contains(a) && keyComp.compare(a, this.from) != 0) {
            throw new Error("a is in this: a=" + a + ", this=" + this);
        }
        if (keyComp.compare(a, b) == 0) {
            if (isSingleton() && keyComp.compare(a, this.from) == 0) {
                return null;
            } else {
                return this;
            }
        }
        if (Node.isOrdered(a, this.from, b)
                && keyComp.compare(this.from, b) != 0) {
            // Range   [-----------)
            //      a-----..
            if (Node.isOrdered(a, this.to, b) && !this.contains(b)) {
                // empty
                return null;
            } else {
                // Range   [-----------)
                //     a------b
                //     -------b          a--
                return new SubRange(b, true, this.to, this.toInclusive);
            }
        } else {
            // Range      [------)
            //       a--b
            //       --b           a-
            return this;
        }
    }*/

    public SubRange[] retainRanges(DdllKey a, DdllKey b) {
        if (keyComp.compare(a, b) != 0 && keyComp.isOrdered(from, b, a)
                && keyComp.compare(from, a) != 0
                && keyComp.isOrdered(b, a, to)
                && keyComp.compare(to, b) != 0) {
            // (k-abe) not sure if this situation actually occurs
            // Range   [---------)
            //       -----b   a------
            // (keyComp.compare(from, a) != 0) がないと，
            // a = from && b = to の場合も真になってしまう．
            return new SubRange[] { new SubRange(b, false, a, false) };
        }
        List<SubRange> retains = new ArrayList<SubRange>();
        if (this.contains(a) && keyComp.compare(a, this.from) != 0) {
            // Range   [---------)
            //             a----..
            retains.add(new SubRange(from, fromInclusive, a, false));
        }
        if (this.contains(b) && keyComp.compare(b, this.to) != 0) {
            // Range   [---------)
            //         ..-----b
            retains.add(new SubRange(b, true, to, toInclusive));
        }
        if (retains.isEmpty()) {
            return null;
        }
        if (true) {
            return retains.toArray(new SubRange[retains.size()]);
        }

        /* old code below */

        // Range   [---------)
        //             a---...
        if (this.contains(a) && keyComp.compare(a, this.from) != 0) {
            // Range   [---------)
            //             a---b
            if (this.contains(b) && keyComp.compare(b, this.to) != 0) {
                return new SubRange[] {
                        new SubRange(from, fromInclusive, a, false),
                        new SubRange(b, false, to, toInclusive) };
            } else {
                // Range   [---------)
                //             a----------b
                return new SubRange[] { new SubRange(from, fromInclusive, a,
                        false) };
            }
        }
        if (keyComp.compare(a, b) == 0) {
            if (isSingleton() && keyComp.compare(a, this.from) == 0) {
                return null;
            } else {
                return new SubRange[] { this };
            }
        }
        if (Node.isOrdered(a, this.from, b)
                && keyComp.compare(this.from, b) != 0) {
            // Range   [-----------)
            //      a-----..
            if (Node.isOrdered(a, this.to, b) && !this.contains(b)) {
                // empty
                return null;
            } else {
                // Range   [-----------)
                //     a------b
                //     -------b          a--
                return new SubRange[] { new SubRange(b, true, this.to,
                        this.toInclusive) };
            }
        } else {
            // Range      [------)
            //       a--b
            //       --b           a-
            return new SubRange[] { this };
        }
    }

    public SubRange concatenate(SubRange another, boolean auxRight) {
        if (this.to.compareTo(another.from) != 0) {
            throw new IllegalArgumentException("not continuous: " + this
                    + " and " + another);
        }
        SubRange kr =
                new SubRange(auxRight ? another.getLink() : getLink(), this.from,
                        this.fromInclusive, another.to, another.toInclusive);
        return kr;
    }

    public void assignId() {
        if (ids == null) {
            ids = new Integer[1];
            ids[0] = (int) (Math.random() * MAXID);
        }
    }

    public void assignSubId(SubRange parent) {
        if (!isSameRange(parent)) {
            Integer[] ids = new Integer[parent.ids.length + 1];
            System.arraycopy(parent.ids, 0, ids, 0, parent.ids.length);
            ids[ids.length - 1] = (int) (Math.random() * MAXID);
            this.ids = ids;
        } else {
            this.ids = parent.ids; // no copy ok?
        }
    }
}
