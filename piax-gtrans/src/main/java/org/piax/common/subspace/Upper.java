/*
 * Upper.java - A class that corresponds to the upper limit
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Upper.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.subspace;

import org.piax.common.ComparableKey;
import org.piax.util.KeyComparator;

/**
 * A class that corresponds to the upper limit
 */
public class Upper<K extends ComparableKey<?>> extends LowerUpper {
    private static final long serialVersionUID = 1L;

    /**
     * @param inclusive
     * @param point
     * @param k
     */
    public Upper(boolean inclusive, K point, int maxNum) {
        super(new KeyRange<ComparableKey<?>>(
                point, inclusive, 
                KeyComparator.getPlusInfinity(point.getClass()), false), 
                true, maxNum);
    }
    
    public Upper(K point, int maxNum) {
        this(true, point, maxNum);
    }

    public Upper(K point) {
        this(true, point, 1);
    }
    
    public boolean isInclusive() {
        return range.fromInclusive;
    }
    
    @SuppressWarnings("unchecked")
    public K getPoint() {
        return (K) range.from;
    }
    
    @Override
    public String toString() {
        String s = isInclusive() ? "]" : ")";
        return getPoint() + s + "<" + getMaxNum() + ">";
    }
}
