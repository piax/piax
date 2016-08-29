/*
 * Link.java - Link implementation of DDLL.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans.ov.ddll;

import java.io.Serializable;

import org.piax.common.Endpoint;

/**
 * a class representing a <em>link</em>.
 * a link consists of a {@link Endpoint} and a {@link DdllKey}.
 * <p> 
 * Note that an instance of <code>DdllKey</code> contains both a key itself
 * and an ID. 
 */
public class Link implements Cloneable, Serializable, Comparable<Link> {
    private static final long serialVersionUID = 1L;

    public final Endpoint addr;
    public final DdllKey key;
    private final int hash;

    public Link(Endpoint node, DdllKey key) {
        this.addr = node;
        if (node == null) {
            throw new NullPointerException();
        }
        this.key = key;
        hash = addr.hashCode() ^ key.hashCode2();
    }

    /**
     * equals() method.
     * Note that `key' field is compared with {@link DdllKey#compareTo(DdllKey)},
     * not {@link DdllKey#equals()}.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Link)) {
            return false;
        }
        Link another = (Link) o;
        return addr.equals(another.addr) && key.compareTo(another.key) == 0;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    /**
     * compareTo() method.  This method compares the key field only.
     */
    @Override
    public int compareTo(Link another) {
        return key.compareTo(another.key);
    }

    @Override
    public synchronized Object clone() {
        try {
            Link t = (Link) super.clone();
            return t;
        } catch (CloneNotSupportedException e) {
            throw new Error(e);
        }
    }

    @Override
    public String toString() {
        String s = addr.toString();
        if (s.startsWith("127.0.0.1:")) {
            s = s.substring(9); // omit "127.0.0.1" for simplicity
        }
        return key.toString() + "@" + s;
    }

    public Link getIdChangedLink(String id) {
        return new Link(addr, key.getIdChangedKey(id));
    }
}
