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

package org.piax.common;

import java.io.Serializable;

import org.piax.util.KeyComparator;
import org.piax.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class representing a key for DDLL.
 * <p>
 * An instance of DdllKey contains a primaryKey, peerId and 
 * id. A PeerId is used for making the DdllKey unique.
 * <p>
 * IDs are used for identifying linked-lists.  Different linked-lists 
 * should have different IDs. 
 * When repairing a linked-list, keys that have the same ID is used as a 
 * hint for finding a live left node.
 * <p>
 * appData is an application-specific data.
 * For example, the skip graph implementation (org.piax.gtrans.ov.sg) uses 
 * this field for passing a membership vector.
 */
public class DdllKey implements Comparable<DdllKey>, Serializable, Cloneable {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(DdllKey.class);

    private static final long serialVersionUID = 1L;
    
    private static KeyComparator keyComp = KeyComparator.getInstance();

    // 主キー
    public final Comparable<?> rawKey;
    final PeerId peerId;
    // 以下は大小比較の際考慮しない．
    // 同一物理ノードの間でキーを識別するための識別子
    public final String id;
    /** application supplied data */
    public final Object appData;
    private final int hash; 

    // a field for repetitive key usage.
    // To distinguish key instances with same key, peerId, and id.
    private final int nonce;

    // private constructor for unsafe operation
    private DdllKey(@SuppressWarnings("unused") boolean dummy,
            Comparable<?> key, PeerId peerId, String id, 
            int nonce, Object appData) {
        this.rawKey = key;
        this.peerId = peerId;
        this.id = id;
        int h = rawKey.hashCode();
        h ^= peerId.hashCode();
        this.nonce = nonce;
        this.hash = h ^ nonce;
        this.appData = appData;
    }

    public DdllKey(Comparable<?> key, PeerId peerId, String id, int nonce,
            Object appData) {
        this(true, key, peerId, id, nonce, appData);
        assert (!peerId.isMinusInfinity() && !peerId.isPlusInfinity());
    }

    public DdllKey(Comparable<?> key, PeerId peerId, String id,
            Object appData) {
        this(true, key, peerId, id,
                (peerId.isMinusInfinity() || peerId.isPlusInfinity()) ?
                        0 : RandomUtil.getSharedRandom().nextInt(),
                        appData);
    }

    public DdllKey(Comparable<?> key, PeerId peerId) {
        this(key, peerId, "", null);
    }

    public DdllKey(Comparable<?> key, PeerId peerId, int nonce) {
        this(key, peerId, "", nonce, null);
    }

    /**
     * get rawKey portion of the key
     * 
     * @return the raw key
     **/
    public Comparable<?> getRawKey() {
        return rawKey;
    }

    /**
     * get PeerID portion of the key
     * 
     * @return the PeerID
     **/
    public PeerId getPeerId() {
        return peerId;
    }
    
    public String getId() {
        return id;
    }

    public int compareTo(DdllKey o) {
        int cmp = keyComp.compare(rawKey, o.rawKey);
        if (cmp != 0) {
//            if (logger.isDebugEnabled()) {
//                logger.debug("compareTo: {}, {} = {}", this, o, cmp);
//            }
            return cmp;
        }
        cmp = peerId.compareTo(o.peerId);
        if (cmp != 0) {
//            if (logger.isDebugEnabled()) {
//                logger.debug("compareTo: {}, {} = {}", this, o, cmp);
//            }
            return cmp;
        }
        cmp = Integer.compare(nonce, o.nonce);
        if (cmp != 0) {
//            if (logger.isDebugEnabled()) {
//                logger.debug("compareTo: {}, {} = {}", this, o, cmp);
//            }
            return cmp;
        }
//        if (logger.isDebugEnabled()) {
//            logger.debug("compareTo: {}, {} = 0", this, o);
//        }
        return 0;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DdllKey)) {
            return false;
        }
        DdllKey o = (DdllKey)obj;
        if (!rawKey.equals(o.rawKey)) {
            return false;
        }
        if (!peerId.equals(o.peerId)) {
            return false;
        }
        if (!id.equals(o.id)) {
            return false;
        }
        if (nonce != o.nonce) {
            return false;
        }
        return true;
    }

    /**
     * compare without id
     * 
     * @param o the key object.
     * @return  comparison results
     */
    public boolean equals2(DdllKey o) {
        if (!rawKey.equals(o.rawKey)) {
            return false;
        }
        if (!peerId.equals(o.peerId)) {
            return false;
        }
        if (nonce != o.nonce) {
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
        if (peerId != null) {
            s = peerId.toString();
            if (s.length() > 4) {
                s = s.substring(0, 4);
            }
        }
        return rawKey.toString() + "!" + s
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
     * @param   id an id to be set
     * @return  the DdllKey with the specified ID
     */
    public DdllKey getIdChangedKey(String id) {
        return new DdllKey(true, rawKey, peerId, id, nonce, appData);
    }
}
