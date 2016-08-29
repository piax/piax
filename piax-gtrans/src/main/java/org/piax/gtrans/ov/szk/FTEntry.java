/*
 * FTEntry.java - A finger table entry implementation.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.szk;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.piax.gtrans.ov.ddll.Link;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * an entry of a finger table
 */
public class FTEntry implements Serializable, Cloneable {
    /*--- logger ---*/
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(FTEntry.class);
    private static final long serialVersionUID = 1L;
    protected Link link;
    protected Link[] successors;

    protected FTEntry(Link link) {
        this.link = link;
    }

    public void setSuccessors(Link[] successors) {
        this.successors = successors;
        //logger.debug("setSuccessor: {}", (Object[])successors);
    }

    @Override
    public String toString() {
        return "[" + link + ", suc=" + Arrays.toString(successors) + "]";
    }

    public Link getLink() {
        return link;
    }

    public Link[] getSuccessors() {
        return successors;
    }

    /**
     * update this entry as the local entry (where its index == LOCALINDEX).
     * 
     * @param vnode
     */
    public void updateLocalEntry(ChordSharpVNode<?> vnode) {
        // empty
    }

    public List<Link> allLinks() {
        List<Link> links = new ArrayList<Link>();
        links.add(link);
        if (successors != null) {
            links.addAll(Arrays.asList(successors));
        }
        return links;
    }

    @Override
    public FTEntry clone() {
        try {
            return (FTEntry) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new Error(e);
        }
    }
}