/*
 * DdllKey.java - DdllKey implementation of DDLL.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: DdllKey.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans.ov.ddll;

import java.io.Serializable;

import org.piax.util.KeyComparator;
import org.piax.util.UniqId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class representing a key for DDLL.
 * <p>
 * An instance of DdllKey contains a {@link #primaryKey}, {@link #uniqId} and 
 * {@link #id}. A UniqId is used for making the DdllKey unique.
 * <p>
 * IDs are used for identifying linked-lists.  Different linked-lists 
 * should have different IDs. 
 * When repairing a linked-list, keys that have the same ID is used as a 
 * hint for finding a live left node.
 * <p>
 * {@link appData} is an application-specific data, which is not used by the
 * DDLL package.  The skip graph implementation {@link org.piax.gtrans.ov.sg} uses 
 * this field for passing a {@link org.piax.gtrans.ov.sg.MembershipVector}. 
 */
public class DdllKey implements Comparable<DdllKey>, Serializable, Cloneable {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(DdllKey.class);

    private static final long serialVersionUID = 1L;
    
    private static KeyComparator keyComp = KeyComparator.getInstance();

    // 主キー
    final Comparable<?> primaryKey;
    final UniqId uniqId;
    // 以下は大小比較の際考慮しない．
    // 同一物理ノードの間でキーを識別するための識別子
    final String id;
    /** application supplied data */
    public final Object appData;
    private final int hash; 
    
    public DdllKey(Comparable<?> key, UniqId uniqId, String id, Object appData) {
        this.primaryKey = key;
        this.uniqId = uniqId;
        this.id = id;
        int h = primaryKey.hashCode();
        if (uniqId != null) {
            h ^= uniqId.hashCode();
        }
        this.hash = h;
        this.appData = appData;
    }

    public DdllKey(Comparable<?> key, UniqId uniqId) {
        this(key, uniqId, "", null);
    }

    /**
     * get primaryKey portion of the key
     * 
     * @return the primary key
     **/
    public Comparable<?> getPrimaryKey() {
        return primaryKey;
    }

    /**
     * get PeerID portion of the key
     * 
     * @return the PeerID
     **/
    public UniqId getUniqId() {
        return uniqId;
    }
    
    public String getId() {
        return id;
    }

    public int compareTo(DdllKey o) {
        int cmp = keyComp.compare(primaryKey, o.primaryKey);
        if (cmp != 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("compareTo: " + this + ", " + o + " = " + cmp);
            }
            return cmp;
        }
        cmp = uniqId.compareTo(o.uniqId);
        if (cmp != 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("compareTo: " + this + ", " + o + " = " + cmp);
            }
            return cmp;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("compareTo: " + this + ", " + o + " = 0");
        }
        return 0;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DdllKey)) {
            return false;
        }
        DdllKey o = (DdllKey)obj;
        if (!primaryKey.equals(o.primaryKey)) {
            return false;
        }
        if (!uniqId.equals(o.uniqId)) {
            return false;
        }
        if (!id.equals(o.id)) {
            return false;
        }
        return true;
    }

    /**
     * compare without id
     * 
     * @param o
     * @return  comparison results
     */
    public boolean equals2(DdllKey o) {
        if (!primaryKey.equals(o.primaryKey)) {
            return false;
        }
        if (!uniqId.equals(o.uniqId)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hash ^ id.hashCode();
    }

    /**
     * hashcode without id
     * 
     * @return hashcode
     */
    public int hashCode2() {
        return hash;
    }


    @Override
    public String toString() {
        String s = "null";
        if (uniqId != null) {
            s = uniqId.toString();
            if (s.length() > 4) {
                s = s.substring(0, 4);
            }
        }
        return primaryKey.toString() + "!" + s
            + (id.equals("") ? "" : "." + id);
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new Error(e);
        }
    }

    /**
     * get a DdllKey instance with the specified ID
     * 
     * @param id    an id to be set
     * @return  the DdllKey with the specified ID
     */
    public DdllKey getIdChangedKey(String id) {
        return new DdllKey(primaryKey, uniqId, id, appData);
    }
}
