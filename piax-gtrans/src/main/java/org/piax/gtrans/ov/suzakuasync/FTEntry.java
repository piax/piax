/*
 * FTEntry.java - A finger table entry implementation.
 * 
 * Copyright (c) 2017 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.suzakuasync;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.piax.gtrans.async.Node;

/**
 * an entry of a finger table
 */
public class FTEntry implements Cloneable, Serializable {
    private final Node link;
    private Node[] nbrs;

    protected FTEntry(Node link) {
        this.link = link;
    }

    public void setNbrs(Node[] nbrs) {
        this.nbrs = nbrs;
    }

    @Override
    public String toString() {
        return "[" + getLink() + ", nbrs=" + Arrays.toString(nbrs) + "]";
    }

    public Node getLink() {
        return link;
    }

    public Node[] getNbrs() {
        return nbrs;
    }

    /**
     * update this entry as the local entry (where its index == LOCALINDEX).
     * 
     * @param vnode
     */
    public void updateLocalEntry(Node vnode) {
        // empty
    }

    public void updateNbrs(FTEntry another) {
        assert this.link == another.link;
        setNbrs(another.getNbrs());
    }

    public List<Node> allLinks() {
        List<Node> links = new ArrayList<Node>();
        links.add(getLink());
        if (nbrs != null) {
            links.addAll(Arrays.asList(nbrs));
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