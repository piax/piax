/*
 * ChordSharpVNode.java - A virtual node of multi-key Chord##.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id$
 */
package org.piax.gtrans.ov.szk;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.Node;
import org.piax.gtrans.ov.ring.NoSuchKeyException;
import org.piax.gtrans.ov.ring.UnavailableException;
import org.piax.gtrans.ov.ring.rq.FlexibleArray;
import org.piax.gtrans.ov.ring.rq.RQVNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a virtual node of multi-key Chord#.
 * 
 * @param <E>  Endpoint of the underlying network
 */
public class ChordSharpVNode<E extends Endpoint> extends RQVNode<E> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(ChordSharpVNode.class);

    public static boolean DBEUG_FT_UPDATES = true;

    public final static int UPDATE_FINGER_PERIOD = 5 * 1000;
    /** parameter to compute the base of log */
    public final static int B = 2; // to reproduce chord# behavior, set this as 1
    /** the base of log. K = 2<sup>B</sup> */
    public final static int K = 1 << B;

    private volatile boolean updatingFT = false;

    public final static int SUCCESSOR_LIST_SIZE = 2;
    static {
        assert SUCCESSOR_LIST_SIZE <= K;
    }

    /** forward finger table */
    FingerTable<E> forwardTable = new FingerTable<E>(this, false);
    /** backward finger table */
    FingerTable<E> backwardTable = new FingerTable<E>(this, true);
    ChordSharp<E> cs;
    ScheduledFuture<?> ftUpdateTask;

    public ChordSharpVNode(ChordSharp<E> cs, Comparable<?> rawkey) {
        super(cs, rawkey);
        this.cs = cs;
    }

    @Override
    public String toStringRoutingTable() {
        StringBuilder buf = new StringBuilder();
        buf.append("|ddll: " + ddllNode).append("\n");
        //buf.append("predecessors: " + getPredecessorList());
        buf.append("|successors: " + getSuccessorList()).append("\n");
        int fftsize = getFingerTableSize();
        FlexibleArray<String> left = new FlexibleArray<String>(-1);
        int MIN = FingerTable.LOCALINDEX;
        int lmax = 1;
        for (int i = MIN; i < fftsize; i++) {
            FTEntry ent = getFingerTableEntry(i);
            left.set(i, ent.toString());
            lmax = Math.max(lmax, left.get(i).length());
        }
        int bftsize = getBackwardFingerTableSize();
        FlexibleArray<String> right = new FlexibleArray<String>(-1);
        right.set(-1, "");
        for (int i = 0; i < bftsize; i++) {
            FTEntry ent = getBackwardFingerTableEntry(i);
            if (ent != null) {
            		right.set(i, ent.toString());
            }
        }
        String fmt0 = "|  |%-" + lmax + "s|%s\n";
        String fmt1 = "|%2d|%-" + lmax + "s|%s\n";
        buf.append(String.format(fmt0, "Forward", "Backward"));
        for (int i = MIN; i < Math.max(fftsize, bftsize); i++) {
            String l = i < fftsize ? left.get(i) : "";
            String r = i < bftsize ? right.get(i) : "";
            buf.append(String.format(fmt1, i, l, r));
        }
        return buf.toString();
    }

    @Override
    protected boolean addKey(E introducer) throws UnavailableException,
            IOException {
        logger.debug("Chord#: addKey is called");
        boolean inserted = super.addKey(introducer);
        if (!inserted) {
            return false;
        }

        // XXX: locking gap OK?
        /*rtLockW();
        // store the 1st finger table entry
        FTEntry ent = new FTEntry();
        ent.link = getSuccessor();
        fingerTable.set(0, ent);
        rtUnlockW();*/

        // copy predecessor's finger table
        ChordSharpIf stub = getStub(getPredecessor().addr);
        try {
            rtLockW();
            FTEntry[][] fts = stub.getFingerTable(getPredecessor().key);
            for (int i = 1; i < fts[0].length; i++) {
                forwardTable.set(i, fts[0][i]);
            }
            for (int i = 1; i < fts[1].length; i++) {
                backwardTable.set(i, fts[1][i]);
            }
        } catch (NoSuchKeyException e) {
            logger.debug("got " + e + ", ignored");
        } catch (RPCException e) {
            logger.debug("got " + e + ", ignored");
        } finally {
            rtUnlockW();
        }
        logger.debug("Chord#: addKey finished: {}", this);

        scheduleFTUpdate(false);
        return true;
    }

    public void scheduleFTUpdate(boolean immed) {
        scheduleFTUpdate(
                immed ? 0 : (int) (UPDATE_FINGER_PERIOD * Math.random()),
                UPDATE_FINGER_PERIOD);
    }

    public void scheduleFTUpdate(int delay, int interval) {
        Runnable run = new Runnable() {
            @Override
            public void run() {
                updateFingerTable();
            }
        };
        rtLockW();
        try {
            if (ftUpdateTask != null) {
                // XXX: cancel(false) doesn't interrupt a running task.
                // It is faster to call cancel(true) to interrupt a running task
                // but it closes shared channel and causes ClosedByInterruptException
                // (2015/11/5 teranisi).
                ftUpdateTask.cancel(false);
                // XXX: 本当は，cancelしたタスクが終了するのを待ってから finger table
                // の更新を行いたいが，ScheduledExecutorService を使っている場合は困難
                // である．おそらく実害はないのでとりあえずこのままとする．(k-abe)
            }
            ftUpdateTask = cs.ftPool.schedule(run, delay, interval);
        } finally {
            rtUnlockW();
        }
    }

    @Override
    protected boolean removeKey() {
        boolean rc = super.removeKey();
        if (rc) {
            rtLockW();
            if (ftUpdateTask != null) {
                ftUpdateTask.cancel(false);
                ftUpdateTask = null;
            }
            rtUnlockW();
        }
        return rc;
    }

    @Override
    public Link[] getAllLinks() {
        List<Link> links = new ArrayList<Link>();
        if (mode != VNodeMode.INSERTED && mode != VNodeMode.DELETING) {
            return links.toArray(new Link[links.size()]);
        }
        links.add(getLocalLink());
        Link pred = getPredecessor();
        if (pred != null) {
            links.add(getPredecessor());
        }
        rtLockR();
        for (int i = 0; i < getFingerTableSize(); i++) {
            FTEntry ent = getFingerTableEntry(i);
            links.add(ent.link);
        }
        rtUnlockR();
        logger.debug("getAllLinks: " + links);
        return links.toArray(new Link[links.size()]);
    }

    public List<Link> getSuccessorList() {
        List<Link> list = new ArrayList<Link>();
        rtLockR();
        int size = Math.min(SUCCESSOR_LIST_SIZE, getFingerTableSize());
        for (int i = 0; i < size; i++) {
            FTEntry ent = getFingerTableEntry(i);
            list.add(ent.link);
        }
        rtUnlockR();
        return list;
    }

    // to be overridden
    protected FTEntry getLocalFTEnetry() {
        return new FTEntry(getLocalLink());
    }

    public int getFingerTableSize() {
        return forwardTable.getFingerTableSize();
    }

    public int getBackwardFingerTableSize() {
        return backwardTable.getFingerTableSize();
    }

    // RPC service
    /**
     * finger table 上で，距離が tk<sup>x</sup> (0 &lt;= t &lt;= 2<sup>y</sup>)
     * 離れたエントリを取得する．結果は 2<sup>y</sup>+1 要素の配列として返す． 
     * 
     * @param x     parameter
     * @param y     parameter
     * @param k     parameter
     * @param given finger table entries to give to the remote node
     * @return list of finger table entries
     */
    public FTEntrySet getFingers(int x, int y, int k, FTEntrySet given) {
        FTEntrySet set = new FTEntrySet();
        // {tk^x | 0 < t <= 2^y}
        // x = floor(p/b), y = p-bx
        // ---> p = y + bx 
        set.ents = new FTEntry[(1 << y) + 1];
        // t = 0 represents the local node and
        // t > 0 represents finger table entries
        for (int t = 0; t <= (1 << y); t++) {
            int d = t * (1 << (B * x));
            int index = FingerTable.getFTIndex(d);
            int d2 = (t + 1) * (1 << (B * x));
            int index2 = FingerTable.getFTIndex(d2);
            FTEntry l = getFingerTableEntryForRemote(index, index2);
            set.ents[t] = l;
        }
        if (ChordSharpVNode.DBEUG_FT_UPDATES) {
            logger.debug("getFingers(x={}, y={}, k={}, given={}) returns {}",
                    x, y, k, given, set);
        }
        int p = y + B * x;
        int distance = 1 << p;
        int index = FingerTable.getFTIndex(distance);
        backwardTable.set(index, given.ents[0]);
        if (p > 0) {
            int index2 = FingerTable.getFTIndex(1 << (p - 1));
            for (int i = 1; i < given.ents.length; i++) {
                backwardTable.set(index2 + i, given.ents[i]);
                if (ChordSharpVNode.DBEUG_FT_UPDATES) {
                    logger.debug("getFingers: BFT[{}] = {}", i, given.ents[i]);
                }
            }
        }
        return set;
    }

    // RPC service
    public FTEntry[][] getFingerTable() {
        rtLockR();
        FTEntry[][] rc = new FTEntry[2][];
        rc[0] = new FTEntry[getFingerTableSize()];
        for (int i = 1; i < getFingerTableSize(); i++) {
            FTEntry ent = getFingerTableEntry(i);
            rc[0][i] = ent;
        }
        rc[1] = new FTEntry[getBackwardFingerTableSize()];
        for (int i = 1; i < getBackwardFingerTableSize(); i++) {
            FTEntry ent = getBackwardFingerTableEntry(i);
            rc[1][i] = ent;
        }
        rtUnlockR();
        return rc;
    }

    final protected FTEntry getFingerTableEntry(int index) {
        return forwardTable.getFTEntry(index);
    }

    /**
     * get a specified FTEntry for giving to a remote node.
     * in aggregation chord#, the range [index, index2) is used as the 
     * aggregation range.
     * this method is intended to be overridden by subclasses.
     * 
     * @param index     index of the entry
     * @param index2    index of the next entry. 
     * @return the FTEntry
     */
    protected FTEntry getFingerTableEntryForRemote(int index, int index2) {
        FTEntry ent = getFingerTableEntry(index);
        //logger.debug("getFTRemote: {}, {}", index, index2);
        if (index == FingerTable.LOCALINDEX) {
            List<Link> succs = getSuccessorList();
            ent.successors = succs.toArray(new Link[succs.size()]);
        }
        return ent;
    }

    protected FTEntry getBackwardFingerTableEntry(int index) {
        return backwardTable.getFTEntry(index);
    }

    void updateFingerTable() {
        rtLockW();
        try {
            if (mode == VNodeMode.OUT || updatingFT) {
                return;
            }
            updatingFT = true;
        } finally {
            rtUnlockW();
        }
        try {
            updateFingerTable0();
        } finally {
            rtLockW();
            updatingFT = false;
            rtUnlockW();
        }
    }

    private void updateFingerTable0() {
        logger.debug("updateFingerTable {}", key);
        for (int p = 0;; p++) {
            if (!manager.isActive()) {
                logger.debug("updateFingerTable: not active.");
                break;
            }
            // finger table上で，距離が 2^p 離れたノードのエントリを取得
            if (ChordSharpVNode.DBEUG_FT_UPDATES) {
                logger.debug("updateFingerTable: p = {}", p);
            }
            int distance = 1 << p;
            int index = FingerTable.getFTIndex(distance);
            rtLockR();
            FTEntry ent;
            try {
                if (mode == VNodeMode.OUT) {
                    break;
                }
                ent = getFingerTableEntry(index);
            } finally {
                rtUnlockR();
            }
            if (ent == null) {
                break;
            }

            // リモートノードに送信するエントリを収集する
            FTEntrySet gift = new FTEntrySet();
            {
                ArrayList<FTEntry> gives = new ArrayList<FTEntry>();
                /* 
                 * |-------------------------------> distance = 2^p
                 * |--------------->                 dist = 2^(p - 1)
                 * |--> delta
                 * N==A==B==...====P===============Q
                 * 
                 * NがQに対してgetFingersを呼ぶ時，A, B, ... を送る．
                 * delta = N-A間の距離
                 *       = K ^ floor((p - 1) / B)
                 *
                 * K = 4 の場合のN0の経路表:
                 *                          (p-1)/B  K^floor((p-1)/B)
                 *  N1  N2  N3      (p=0, 1)     0          1
                 *  N4  N8 N12      (p=2, 3)     1          4
                 * N16 N32 N48      (p=4, 5)     2          8
                 */
                int delta, max;
                if (p == 0) {
                    delta = 1;
                    max = 1;
                } else {
                    delta = 1 << ((p - 1) / B * B);
                    max = 1 << (p - 1);
                }
                for (int d = 0; d < max; d += delta) {
                    int idx = FingerTable.getFTIndex(d);
                    int idx2 = FingerTable.getFTIndex(d + delta);
                    FTEntry e = getFingerTableEntryForRemote(idx, idx2);
                    // dis = 当該エントリから Q までの距離
                    int dis = distance - d;
                    if (dis < K) {
                        e = e.clone();
                        e.setSuccessors(null);
                    }
                    // 先頭が自ノード，以降は逆順になるようにリストに格納する．
                    // 上の例の場合，gives = {N, C, B, A} となる．
                    // TODO: 自然な順序で格納できるようにgetFingersを修正するべき
                    if (gives.size() == 0) {
                        gives.add(e);
                    } else {
                        gives.add(1, e);
                    }
                }
                //logger.debug("gives = {}", gives);
                gift.ents = gives.toArray(new FTEntry[gives.size()]);
            }

            // 取得するエントリを指定するパラメータ (論文参照)
            int x = p / B;
            int y = p - B * x;
            // リモートノードからfinger tableエントリを取得
            ChordSharpIf stub = getStub(ent.link.addr);
            FTEntrySet set;
            try {
                set = stub.getFingers(ent.link.key, x, y, K, gift);
            } catch (NoSuchKeyException e) {
                // とりあえず無視
                logger.debug("got " + e + ", ignored");
                continue;
            } catch (RPCException e) {
                // とりあえず無視
                logger.debug("got " + e + ", ignored");
                continue;
            } catch (Throwable e) {
                logger.debug("got ", e);
                throw new Error(e);
            }
            if (set == null) {
                // XXX: PIAXのRPCは，Thread#interrupt()を呼んでも
                // InterruptedExceptionをスローせずに，nullが返る模様．
                // この振る舞いはあまり良くない．
                logger.debug("getFingers returns null (thread interrupted?)");
                break;
            }
            FTEntry[] ents = set.ents;
            logger.debug("obtained set = {}", set);
            if (ents != null) {
                // 取得したエントリをfinger tableにセットする
                // the first entry (m=0) represents the remote node, 
                // pointed by ent.link. we retrieve the successor list and
                // sets to ent.
                for (int m = 0; m < ents.length; m++) {
                    FTEntry e = ents[m];
                    if (m == 0) {
                        // this entry represents the node pointed by ent.link.
                        // only the successor part is used.
                        if (e == null) {
                            logger.error("getFingers() returns illegal results");
                        } else {
                            // copy the successor list as backup nodes
                            // ent.setSuccessors(e.successors);
                            e.link = ent.link;
                            forwardTable.set(index, e);
                        }
                    } else {
                        if (e == null
                                || Node.isOrdered(ent.link.key, true, key,
                                        e.link.key, true)) {
                            // エントリがない，あるいは一周したら終了
                            rtLockW();
                            forwardTable.shrink(index + 1 + m);
                            rtUnlockW();
                            break;
                        }
                        forwardTable.set(index + m, e);
                    }
                }
            }
        }
        if (ChordSharpVNode.DBEUG_FT_UPDATES) {
            logger.debug("finger table updated: {}", this);
        }
    }

    private ChordSharpIf getStub(Endpoint e) {
        ChordSharpIf stub = (ChordSharpIf) manager.getStub(e);
        return stub;
    }

    /**
     * a class for sending/receiving finger table entries between nodes.
     */
    public static class FTEntrySet implements Serializable {
        private static final long serialVersionUID = 1L;
        FTEntry[] ents;

        //Link[] predecessors;
        //Link[] successors;

        @Override
        public String toString() {
            //return "[ents=" + Arrays.deepToString(ents) + ", predecessors="
            //        + Arrays.toString(predecessors) + "]";
            //return "[ents=" + Arrays.deepToString(ents) + ", successors="
            //+ Arrays.toString(successors) + "]";
            return "[ents=" + Arrays.deepToString(ents) + "]";
        }
    }

    /*
     * 右ノードが代わったので，finger tableを更新する．
     */
    @Override
    public void onRightNodeChange(Link prevRight, Link newRight, Object payload) {
        super.onRightNodeChange(prevRight, newRight, payload);
        scheduleFTUpdate(true);
    }

    @Override
    public List<Link> suppplyLeftCandidatesForFix() {
        Set<Link> s = new HashSet<Link>();
        FTEntry[][] ents = getFingerTable();
        for (int i = 1; i >= 0; i--) {
            for (int j = 0; j < ents[i].length; j++) {
                FTEntry ent = ents[i][j];
                if (ent != null) {
                    s.addAll(ent.allLinks());
                }
            }
        }
        logger.debug("supply: {}", s);
        return new ArrayList<Link>(s);
    }
}
