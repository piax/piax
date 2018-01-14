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
package org.piax.gtrans.ov.szk;

import org.piax.common.Endpoint;
import org.piax.gtrans.ov.ring.rq.FlexibleArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Finger Table in base K.
 * 
 * There are two special entries that are always available.
 * <pre>
 * index -1: local node.
 * index  0: the successor or predecessor.
 * </pre>
 * @param <E> endpoint
 *  
 */
public class FingerTable<E extends Endpoint> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(FingerTable.class);
    public final static int B = ChordSharpVNode.B;
    public final static int K = ChordSharpVNode.K;
    // index of the local entry.  this index is only available in forward
    // finger tables.
    public final static int LOCALINDEX = -1;
    final ChordSharpVNode<E> vnode;
    final FlexibleArray<FTEntry> table;
    final boolean isBackward;

    public FingerTable(ChordSharpVNode<E> vnode, boolean isBackward) {
        this.vnode = vnode;
        this.isBackward = isBackward;
        if (isBackward) {
            this.table = new FlexibleArray<FTEntry>(0);
        } else {
            this.table = new FlexibleArray<FTEntry>(LOCALINDEX);
            // we have to use distinct instances, local1 and local2, because
            // they will be modified.
            FTEntry local1 = ((ChordSharpVNode<E>)vnode).getLocalFTEnetry();
            // -1th entry is the local node 
            set(LOCALINDEX, local1);
        }
        FTEntry local2 = ((ChordSharpVNode<E>)vnode).getLocalFTEnetry();
        // 0th entry is the successor or predecessor
        set(0, local2);
    }

    public void set(int index, FTEntry ent) {
        table.set(index, ent);
        logger.debug("FingerTable#set index={}, ent={}", index, ent);
    }

    public int getFingerTableSize() {
        return table.maxIndexPlus1();
    }

    FTEntry getFTEntry(int index) {
        FTEntry ent = table.get(index);
        if (index == LOCALINDEX) {
            ent.updateLocalEntry(vnode);
        } else if (index == 0) {
            // the successor and predecessor are managed by DDLL
            /*
             * successorはDDLLで管理しているのに対し，
             * successor-listは，Chord#側で管理している．
             * このため，successorが変更された後，新しいノードからsuccessor-listを取得
             * するまでの期間は，successorとsuccessor-listとの間で齟齬が生じる可能性が
             * あることに注意．
             */
            if (isBackward) {
                ent.link = vnode.getPredecessor();
            } else {
                ent.link = vnode.getSuccessor();
            }
        }
        return ent;
    }

    public void shrink(int index) {
        table.shrink(index);
    }

    public static int getFTIndex(int i, int j) {
        assert i >= 0;
        assert j >= 1;
        return (K - 1) * i + (j - 1);
    }

    public static int getFTIndex(int distance) {
        if (distance == 0) {
            return LOCALINDEX;
        }
        for (int x = 0;; x++) {
            int i = x / (K - 1);
            int j = x - (K - 1) * i + 1;
            int d = j * (1 << (B * i));
            if (distance == d) {
                return x;
            }
            if (distance < d) {
                return x - 1;
            }
        }
    }
}
