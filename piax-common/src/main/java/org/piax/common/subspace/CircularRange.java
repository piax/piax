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
}
