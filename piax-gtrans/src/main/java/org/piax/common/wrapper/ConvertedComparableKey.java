/*
 * ConvertedComparableKey.java - A key wrapper of converted comparable type.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: ConvertedComparableKey.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.common.wrapper;

import org.piax.common.ComparableKey;
import org.piax.common.Key;

/**
 * A key wrapper of converted comparable type.
 */
public class ConvertedComparableKey<K extends Key> implements
        ComparableKey<ConvertedComparableKey<K>> {
    private static final long serialVersionUID = 1L;

    public final K key;
    
    public ConvertedComparableKey(K key) {
        if (key == null)
            throw new IllegalArgumentException("argument should not be null");
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (getClass() != o.getClass())
            return false;
        return key.equals(((ConvertedComparableKey<?>) o).key);
    }

    @SuppressWarnings("unchecked")
    public int compareTo(ConvertedComparableKey<K> o) {
        if (key instanceof Comparable<?>) {
            return ((Comparable<K>) key).compareTo(o.key);
        }
        // TODO このままの実装では推移律が成り立たない
        // equalsが偽で、hashCodeが一致した場合は、byte列自体を比較するしかない
        if (key.equals(o.key)) return 0;
        return (key.hashCode() < o.key.hashCode())? -1 : 1;
    }
    
    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return "(" + key.toString() + ")";
    }
}
