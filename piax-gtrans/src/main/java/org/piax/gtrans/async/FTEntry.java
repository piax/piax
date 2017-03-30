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
import java.util.Objects;
import java.util.stream.Collectors;

import org.piax.gtrans.ov.async.rq.RQAdapter;
import org.piax.gtrans.ov.async.suzaku.SuzakuStrategy;
import org.piax.gtrans.ov.ring.rq.DdllKeyRange;

/**
 * an entry of a finger table
 */
public class FTEntry implements Cloneable, Serializable {
    final boolean isLocal;
    private List<Node> nodes = new ArrayList<>();
    private DdllKeyRange range;
    private Map<Class<? extends RQAdapter<?>>, Object> extData = null;

    public FTEntry(LocalNode node, boolean isLocal) {
        nodes.add(node);
        this.isLocal = true;
    }

    public FTEntry(Node node) {
        nodes.add(node);
        this.isLocal = false;
    }

    public FTEntry(List<Node> nodes) {
        this.nodes.addAll(nodes);
        this.isLocal = false;
    }
    
    // XXX: このクラスでRQAdapterを使うのは良くない
    public void putCollectedData(Class<? extends RQAdapter<?>> clazz,
            Object data) {
        if (extData == null) {
            extData = new HashMap<>();
        }
        extData.put(clazz, data);
    }

    public Object getLocalCollectedData(Class<? extends RQAdapter<?>> clazz) {
        if (isLocal) {
            LocalNode local = (LocalNode)getNode();
            return local.getTopStrategy().getLocalCollectedData(clazz);
        }
        if (extData == null) {
            return null;
        }
        return extData.get(clazz);
    }

    public DdllKeyRange getRange() {
        if (isLocal) {
            LocalNode local = (LocalNode)getNode();
            return new DdllKeyRange(local.key, true, local.succ.key, false);
        }
        return range;
    }

    public void setRange(DdllKeyRange range) {
        this.range = range;
    }

    @Override
    public String toString() {
        if (nodes.size() > 1) {
            List<Node> nbrs = nodes.subList(1, nodes.size());
            return "[" + getNode() + ", nbrs=" + nbrs
                    + ", range=" + range
                    + ", data=" + toStringExtData()
                    + "]";
        }
        return "[" + getNode()
            + ", range=" + range
            + ", data=" + toStringExtData()
            + "]";
    }
    
    private String toStringExtData() {
        if (extData == null) {
            return "null";
        }
        List<String> list = extData.entrySet().stream()
            .map(e -> {
                if (e.getValue() == null) {
                    return null;
                }
                String s = e.getKey().getSimpleName();
                s = s.substring(0, Math.min(6, s.length()));
                return s + "->" + e.getValue();
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        return "[" + String.join(", ", list) + "]";
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
