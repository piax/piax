/*
 * KeyRange.java - A class that corresponds to the range of key
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: KeyRange.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.common.subspace;

import org.piax.common.ComparableKey;

/**
 * A class that corresponds to the range of key
 */
public class KeyRange<K extends ComparableKey<?>> extends Range<K> implements
        KeyContainable<ComparableKey<?>> {
    private static final long serialVersionUID = 1L;

    public KeyRange(K from, K to) {
        super(from, to);
    }

    public KeyRange(K key) {
        super(key);
    }

    public KeyRange(K from, boolean fromInclusive, K to, boolean toInclusive) {
        super(from, fromInclusive, to, toInclusive);
    }

    @Override
    public boolean contains(ComparableKey<?> key) {
        return super.contains(key);
    }
}
