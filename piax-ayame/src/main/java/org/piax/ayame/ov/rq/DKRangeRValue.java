/*
 * DKRangeRValue.java - An implementation of DdllKeyRange with a RemoteValue.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.ayame.ov.rq;

import org.piax.ayame.ov.ddll.DdllKey;
import org.piax.ayame.ov.ddll.DdllKeyRange;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;

/**
 * DdllKeyRange with a RemoteValue
 * @param <V> the range value.
 */
public class DKRangeRValue<V> extends DdllKeyRange {
    final RemoteValue<V> value;

    public DKRangeRValue(RemoteValue<V> value, DdllKey from,
            boolean fromInclusive, DdllKey to, boolean toInclusive) {
        super(from, fromInclusive, to, toInclusive);
        this.value = value;
    }

    public DKRangeRValue(RemoteValue<V> value, Range<DdllKey> range) {
        super(range);
        this.value = value;
    }
    
    public RemoteValue<V> getRemoteValue() {
        return value;
    }
    
    @Override
    public String toString() {
        return "[DKRangeRValue " + super.toString() + " " 
                 + (value == null ? "null" : value.toString()) 
                + "]";
    }
}
