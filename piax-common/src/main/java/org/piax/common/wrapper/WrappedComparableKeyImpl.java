/*
 * WrappedComparableKeyImpl.java - A common implementation of wrapped keys.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: WrappedComparableKeyImpl.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.common.wrapper;

import org.piax.util.Exception4StackTracing;
import org.piax.util.KeyComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A common implementation of wrapped keys.
 */
public abstract class WrappedComparableKeyImpl<K extends Comparable<?>>
        implements WrappedComparableKey<K> {
    private static final long serialVersionUID = 1L;

    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(WrappedComparableKeyImpl.class);

    private static final KeyComparator keyComp = KeyComparator.getInstance();

    private final K key;
    
    protected WrappedComparableKeyImpl(K key) {
        if (key == null)
            throw new IllegalArgumentException("argument should not be null");
        if (key instanceof WrappedComparableKeyImpl) {
            logger.debug("key:{} is already WrappedComparableKey", key);
            logger.trace("", new Exception4StackTracing());
        }
        this.key = key;
    }

    public K getKey() {
        return key;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (getClass() != o.getClass())
            return false;
        return key.equals(((WrappedComparableKeyImpl<?>) o).key);
    }

    @Override
    public int compareTo(WrappedComparableKey<K> o) {
        // key.compareTo(o.key); ではなく、KeyComparatorを使っている点に注意
        return keyComp.compare(key, o.getKey());
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
