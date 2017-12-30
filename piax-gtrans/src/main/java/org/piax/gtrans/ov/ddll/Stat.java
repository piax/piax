/*
 * Stat.java - A container class for storing node states.
 *
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: OfflineSendException.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ddll;

import java.io.Serializable;

import org.piax.ayame.ov.ddll.DdllKey;
import org.piax.ayame.ov.ddll.LinkNum;
import org.piax.common.Endpoint;
import org.piax.gtrans.ov.ddll.Node.Mode;

/**
 * a container class for storing node states.
 * 
 * @see NodeManagerIf#setStat(DdllKey, int, Stat)
 * @see NodeManagerIf#setStatMulti(Endpoint, Stat[])
 */
public class Stat implements Serializable {
    private static final long serialVersionUID = 1L;
    final DdllKey key;
    final Mode mode;
    final Link me;
    final Link left;
    final Link right;
    final LinkNum rNum;

    /**
     * constructor
     * 
     * @param mode the mode.
     * @param me the link.
     * @param left the left link.
     * @param right the right link.
     * @param rNum the link number.
     */
    Stat(Mode mode, Link me, Link left, Link right, LinkNum rNum) {
        this.key = me.key;
        this.mode = mode;
        this.me = me;
        this.left = left;
        this.right = right;
        this.rNum = rNum;
    }

    /**
     * constructor for an instance that indicates no such key is available
     * 
     * @param key   the key
     */
    Stat(DdllKey key) {
        this.key = key;
        this.mode = null;
        this.me = null;
        this.left = null;
        this.right = null;
        this.rNum = null;
    }

    @Override
    public String toString() {
        return "[mode=" + mode + ", left=" + left + ", me=" + me
                + ", right=" + right + ", rNum=" + rNum + "]";
    }
}
