/*
 * Lower.java - A class that corresponds to the lower limit
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Lower.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.subspace;

import org.piax.common.ComparableKey;
import org.piax.util.KeyComparator;

/**
 * A class that corresponds to the range lower than 
 */
public class Lower<K extends ComparableKey<?>> extends LowerUpper {
    private static final long serialVersionUID = 1L;

    /**
     * @param inclusive
     * @param point
     * @param k
     */
    public Lower(boolean inclusive, K point, int maxNum) {
        super(new KeyRange<ComparableKey<?>>(
                KeyComparator.getMinusInfinity(point.getClass()), false, 
                point, inclusive), false, maxNum);
    }
    
    public Lower(K point, int maxNum) {
        this(true, point, maxNum);
    }

    public Lower(K point) {
        this(true, point, 1);
    }

    public boolean isInclusive() {
        return range.toInclusive;
    }
    
    @SuppressWarnings("unchecked")
    public K getPoint() {
        return (K) range.to;
    }

    @Override
    public String toString() {
        String s = isInclusive() ? "[" : "(";
        return "<" + getMaxNum() + ">" + s + getPoint();
    }
}
