/*
 * LinkSeq.java - LinkSeq implementation of DDLL.
 * 
 * Copyright (c) 2009-2018 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: LinkSeq.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.ayame.ov.ddll;

import java.io.Serializable;


/**
 * a link sequence number used by DDLL algorithm 
 */
public class LinkSeq implements Serializable, Comparable<LinkSeq> {
    private static final long serialVersionUID = 1L;
    private final int repair;
    private final int seq;

    public LinkSeq(int repair, int seq) {
        this.repair = repair;
        this.seq = seq;
    }
    
    public LinkSeq gnext() {
        return new LinkSeq(repair + 1, 0);
    }
    
    public LinkSeq next() {
        return new LinkSeq(repair, seq + 1);
    }

    public int compareTo(LinkSeq o) {
        if (this.repair < o.repair) return -1; 
        if (this.repair > o.repair) return 1;
        // this.repair == o.repair
        if (this.seq < o.seq) return -1;
        if (this.seq > o.seq) return 1;
        // this.repair == o.repair && this.seq == o.seq
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LinkSeq) {
            return repair == ((LinkSeq) o).repair &&
                seq == ((LinkSeq) o).seq;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return repair * 97 + seq;
    }

    @Override
    public String toString() {
        return "(" + repair + ", " + seq + ")";
    }
}
