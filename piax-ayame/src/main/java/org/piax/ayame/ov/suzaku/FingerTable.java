/*
 * FingerTable.java - A finger table implementation.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.ayame.ov.suzaku;

import java.util.stream.Stream;

import org.piax.ayame.EventExecutor;
import org.piax.ayame.FTEntry;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;
import org.piax.util.FlexibleArray;

/**
 * A Finger Table in base K.
 * 
 * There are two special entries that are always available.
 * <pre>
 * index -1: local node.
 * index  0: the successor or predecessor.
 * </pre>
 */
public class FingerTable {
    //public final static int B = SuzakuStrategy.B;
    //public final static int K = SuzakuStrategy.K;
    // index of the local entry.  this index is only available in forward
    // finger tables.
    public final static int LOCALINDEX = -1;
    final LocalNode vnode;
    final SuzakuStrategy suzakuStr;
    final FlexibleArray<FTEntry> table;
    final boolean isBackward;
    final FingerTables tables;

    FingerTable(FingerTables tables, LocalNode vnode, boolean isBackward) {
        this.tables = tables;
        this.vnode = vnode;
        this.suzakuStr = SuzakuStrategy.getSuzakuStrategy(vnode);
        this.isBackward = isBackward;
        this.table = new FlexibleArray<FTEntry>(LOCALINDEX);
        // we have to use distinct instances, local1 and local2, because
        // they will be modified.
        FTEntry local1 = new FTEntry(vnode, true);
        // -1th entry is the local node 
        set(LOCALINDEX, local1);
        FTEntry local2 = new FTEntry(vnode);
        // 0th entry is the successor or predecessor
        set(0, local2);
    }

    public void set(int index, FTEntry ent) {
        set(index, ent, true);
    }

    public void set(int index, FTEntry ent, boolean addtorev) {
        table.set(index, ent);
        ent.time = EventExecutor.getVTime();
        // XXX: should replace other entries that points to the same node
        if (addtorev && ent != null && ent.getNode() != null) {
            tables.addReversePointer(ent.getNode());
        }
    }

    /**
     * update a finger table entry.
     * 
     * @param index index to update
     * @param ent entry to replace
     * @param addtorev true if ent should be added to the reverse pointer set.
     * @return true if RemoveReversePointerEvent is sent
     */
    public boolean change(int index, FTEntry ent, boolean addtorev) {
        assert index != LOCALINDEX;
        FTEntry old = getFTEntry(index);
        //System.out.println(vnode + ": change: index=" + index + ", " + old + " to " + ent);
        if (index == 0) {
            // replace to `ent' to update `range' portion if index == 0.
            // note that level 0 pointers are managed by DDLL.
            if (old.getNode() == ent.getNode()) {
                set(0, ent, false);
            }
        } else {
            set(index, ent, addtorev);
        }
        if (old != null && ent != null) {
            if (old.getNode() != null && old.getNode() != ent.getNode()) {
                //System.out.println(vnode + ": ptr changed, index=" + index + " from " + old + " to " + ent);
                suzakuStr.cleanRemoteRevPtr(old.getNode());
                return true;
            }
        }
        return false;
    }

    /**
     * replace a FTEntry without sending cleanRemoteRevPtr.
     * 
     * @param oldEnt
     * @param newEnt
     */
    void replace(Node node, FTEntry newEnt) {
        int size = getFingerTableSize();
        // because level 0 is managed by DDLL, we start iteration from level 1
        for (int i = 1; i < size; i++) {
            FTEntry ent = getFTEntry(i);
            if (ent != null && ent.getNode() == node) {
                set(i, newEnt, true);
            }
        }
    }

    public int getFingerTableSize() {
        return table.maxIndexPlus1();
    }

    FTEntry getFTEntry(int index) {
        FTEntry ent = table.get(index);
        if (index == 0) {
            // successor and predecessor are managed by the lower layer
            Node latest = isBackward ? vnode.pred : vnode.succ;
            if (latest == null) {
                return null;
            }
            if (ent.getNode() != latest) {
                ent = new FTEntry(latest);
                set(0, ent);
            }
        }
        return ent;
    }
    
    /**
     * get a stream of valid FTEntry
     * 
     * @return stream of valid FTEntry
     */
    Stream<FTEntry> stream() {
        Stream.Builder<FTEntry> builder = Stream.builder();
        int size = getFingerTableSize();
        for (int i = 0; i < size; i++) {
            FTEntry ent = getFTEntry(i); 
            if (ent != null && ent.getNode() != null) builder.add(ent);
        }
        return builder.build();
    }

    public void shrink(int index) {
        for (int i = getFingerTableSize() - 1; i >= index; i--) {
            FTEntry ent = getFTEntry(i);
            if (ent != null && ent.getNode() != null) {
                suzakuStr.cleanRemoteRevPtr(ent.getNode());
            }
        }
        table.shrink(index);
    }

    public static int getFTIndex(int i, int j) {
        assert i >= 0;
        assert j >= 1;
        return (SuzakuStrategy.K - 1) * i + (j - 1);
    }

    public static int getFTIndex(int distance) {
        assert distance >= 0;
        if (distance == 0) {
            return LOCALINDEX;
        }
        for (int x = 0;; x++) {
            int d = indexToDistance(x);
            if (distance == d) {
                return x;
            }
            if (distance < d) {
                return x - 1;
            }
        }
    }

    public static int indexToDistance(int index) {
        if (index == -1) {
            return 0;
        }
        int i = index / (SuzakuStrategy.K - 1);
        int j = index - (SuzakuStrategy.K - 1) * i + 1;
        int d = j * (1 << (SuzakuStrategy.B.value() * i));
        return d;
    }
}
