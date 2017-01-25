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
import java.util.List;

import org.piax.gtrans.async.Node;

/**
 * an entry of a finger table
 */
public class FTEntry implements Cloneable, Serializable {
    private List<Node> nodes = new ArrayList<>();

    protected FTEntry(Node node) {
        nodes.add(node);
    }

    protected FTEntry(List<Node> nodes) {
        this.nodes.addAll(nodes);
    }

    @Override
    public String toString() {
        return "[" + getLink() + ", nbrs=" + nodes + "]";
    }

    // XXX: suspectedなノードを考慮!!!
    public Node getLink() {
        if (nodes.size() > 0) {
            return nodes.get(0);
        }
        return null;
    }

    // XXX:
    boolean needUpdate() {
        return (nodes.size() < SuzakuStrategy.SUCCESSOR_LIST_SIZE);
    }

    /**
     * update this entry as the local entry (where its index == LOCALINDEX).
     * 
     * @param vnode
     */
    public void updateLocalEntry(Node vnode) {
        // empty
    }

    public List<Node> allLinks() {
        return nodes;
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