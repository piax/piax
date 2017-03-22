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
package org.piax.gtrans.async;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.piax.gtrans.ov.async.rq.RQValueProvider;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy;

/**
 * an entry of a finger table
 */
public class FTEntry implements Cloneable, Serializable {
    private List<Node> nodes = new ArrayList<>();
    private Map<Class<? extends RQValueProvider<?>>, Object> extData = null;

    public FTEntry(Node node) {
        nodes.add(node);
    }

    public FTEntry(List<Node> nodes) {
        this.nodes.addAll(nodes);
    }
    
    public <T> void putCollectedData(
            Class<? extends RQValueProvider<T>> ext, T data) {
        if (extData == null) {
            extData = new HashMap<>();
        }
        extData.put(ext, data);
    }

    @SuppressWarnings("unchecked")
    public <T> T getCollectedData(
            Class<? extends RQValueProvider<T>> clazz) {
        if (extData == null) {
            return null;
        }
        return (T) extData.get(clazz);
    }
    
    public Map<Class<? extends RQValueProvider<?>>, Object> getCollectedDataAll() {
        return extData;
    }

    @Override
    public String toString() {
        if (nodes.size() > 1) {
            List<Node> nbrs = nodes.subList(1, nodes.size());
            return "[" + getNode() + ", nbrs=" + nbrs + "]";
        }
        return "[" + getNode() + "]";
    }

    public Node getNode() {
        if (nodes.size() > 0) {
            return nodes.get(0);
        }
        return null;
    }

    // XXX:
    public boolean needUpdate() {
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

    public List<Node> allNodes() {
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
