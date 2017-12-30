/*
 * NamedKey.java - A key wrapper of name (identifier).
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: NamedKey.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.common.wrapper;

import org.piax.common.ComparableKey;
import org.piax.common.ObjectId;

/**
 * A key wrapper of name (identifier)
 */
public abstract class NamedKey<K extends Comparable<?>> implements
        ComparableKey<NamedKey<K>> {
    private static final long serialVersionUID = 1L;

    public final ObjectId name;
    public final K key;
    
    transient int hash; // cache

    protected NamedKey(ObjectId name, K key) {
        if (name == null || key == null)
            throw new IllegalArgumentException("should not be null");
        this.name = name;
        this.key = key;
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            hash = 31 * name.hashCode() + key.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (getClass() != o.getClass())
            return false;
        if (hashCode() != o.hashCode())
            return false;
        NamedKey<?> other = (NamedKey<?>) o;
        if (!key.equals(other.key))
            return false;
        if (!name.equals(other.name))
            return false;
        return true;
    }

    @Override
    public int compareTo(NamedKey<K> o) {
        if (name.equals(o.name)) {
            if (key.getClass() == o.key.getClass()) {
                @SuppressWarnings("unchecked")
                // K extends Comparable<? super K> とするためのcast
                Comparable<? super K> k = (Comparable<? super K>) key;
                return k.compareTo(o.key);
            } else {
                return key.getClass().getName()
                        .compareTo(o.key.getClass().getName());
            }
        } else {
            return name.compareTo(o.name);
        }
    }

    @Override
    public String toString() {
        return "NamedKey[name=" + name + ", key=" + key + "]";
    }
}
