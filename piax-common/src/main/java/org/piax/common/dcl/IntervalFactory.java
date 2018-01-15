/*
 * IntervalFactory.java - An interval factory class of DCL.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: IntervalFactory.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.dcl;

import org.piax.common.ComparableKey;
import org.piax.common.subspace.KeyRange;
import org.piax.common.wrapper.Keys;
import org.piax.util.KeyComparator;

/**
 * An interval factory class of DCL.
 */
public class IntervalFactory implements DCLFactory {
    final boolean fromInclusive;
    final boolean toInclusive;
    ComparableKey<?> from = null;
    ComparableKey<?> to = null;
    boolean isFrom = true;
    
    IntervalFactory(String op) {
        fromInclusive = op.charAt(0) == '[';
        toInclusive = op.charAt(1) == ']';
    }

    ComparableKey<?> convert(Object element) {
        if (element == null) {
            return null;
        } else if (element instanceof ComparableKey<?>) {
            return (ComparableKey<?>) element;
        } else if (element instanceof Comparable<?>) {
            return Keys.newWrappedKey((Comparable<?>) element);
        } else {
            throw new DCLParseException(element.getClass().getSimpleName() 
                    + ": interval params should be Comparable");
        }
    }
    
    @Override
    public void add(Object element) {
        if (element == null) {
            isFrom = false;
            return;
        }
        if (isFrom) {
            from = convert(element);
        } else { 
            to = convert(element);
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Object getDstCond() {
        if (from == null) {
            from = KeyComparator.getMinusInfinity(to.getClass());
        }
        if (to == null) {
            to = KeyComparator.getPlusInfinity(from.getClass());
        }
        try {
            return new KeyRange(from, fromInclusive, to, toInclusive);
        } catch (IllegalArgumentException e) {
            throw new DCLParseException(e);
        }
    }
}
