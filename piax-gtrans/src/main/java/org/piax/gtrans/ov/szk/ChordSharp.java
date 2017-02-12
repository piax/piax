/*
 * ChordSharp.java - An implementation of multi-key Chord##.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.piax.common.Endpoint;
import org.piax.common.Id;
import org.piax.common.TransportId;
import org.piax.common.subspace.CircularRange;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.Node;
import org.piax.gtrans.ov.ring.MyThreadPool;
import org.piax.gtrans.ov.ring.NoSuchKeyException;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;
import org.piax.gtrans.ov.ring.rq.MessagePath;
import org.piax.gtrans.ov.ring.rq.QueryId;
import org.piax.gtrans.ov.ring.rq.RQAlgorithm;
import org.piax.gtrans.ov.ring.rq.RQExecQueryCallback;
import org.piax.gtrans.ov.ring.rq.RQManager;
import org.piax.gtrans.ov.ring.rq.RQMessage;
import org.piax.gtrans.ov.ring.rq.RQReturn;
import org.piax.gtrans.ov.ring.rq.SubRange;
import org.piax.gtrans.ov.szk.ChordSharpVNode.FTEntrySet;
import org.piax.util.StrictMap;
import org.piax.util.UniqId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * multi-key Chord#
 * 
 * @param <E> the type of Endpoint in the underlying network.
 */
public class ChordSharp<E extends Endpoint> extends RQManager<E> implements
        ChordSharpIf {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(ChordSharp.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("chord#");

    Map<QueryId, Set<Integer>> queryHistory =
            new ConcurrentHashMap<QueryId, Set<Integer>>();

    public final static int FINGER_TABLE_UPDATE_CONCURRENCY = 1;
    /**
     * a dedicated thread pool for updating a finger table
     * to limit max concurrency of finger table updates.
     */
    MyThreadPool ftPool = new MyThreadPool(FINGER_TABLE_UPDATE_CONCURRENCY,
            "ftPool", getPeerId().toString());

    /**
     * ResponseMessageによって通知された，削除されているリモートキーの集合
     */
    // TODO: DdllKeyがrawkey+peerIdだけだと，同一キーを再挿入した時にまずい．
    // TODO: purge later!
    Set<DdllKey> unavailableRemoteKeys = Collections
            .newSetFromMap(new ConcurrentHashMap<DdllKey, Boolean>());

    public ChordSharp(TransportId transId, ChannelTransport<E> trans,
            RQExecQueryCallback execQueryCallback) throws IdConflictException,
            IOException {
        super(transId, trans, execQueryCallback);
        setRQAlgorithm(new ChordSharpRQAlgorithm<E>(this));
    }

    @Override
    protected ChordSharpVNode<E> newVNode(Comparable<?> rawkey,
            Object... params) {
        logger.debug("newVNode is created, key={}", rawkey);
        return new ChordSharpVNode<E>(this, rawkey);
    }

    @Override
    public FTEntrySet getFingers(DdllKey key, int x, int y, int k,
            FTEntrySet given) throws NoSuchKeyException {
        ChordSharpVNode<E> vnode =
                (ChordSharpVNode<E>) keyHash.get(key.getPrimaryKey());
        if (vnode == null) {
            throw new NoSuchKeyException("getFingers: no such key: " + key);
        }
        return vnode.getFingers(x, y, k, given);
    }

    @Override
    public FTEntry[][] getFingerTable(DdllKey key) throws NoSuchKeyException {
        ChordSharpVNode<E> vnode =
                (ChordSharpVNode<E>) keyHash.get(key.getPrimaryKey());
        if (vnode == null) {
            throw new NoSuchKeyException("getFingerTable: no such key: " + key);
        }
        return vnode.getFingerTable();
    }
    
    Link getLocal(Comparable<?> key) {
        ChordSharpVNode<E> vnode = (ChordSharpVNode<E>) keyHash.get(key);
        return vnode.getLocalLink();
    }
    
    Link[] getNeighbors(Comparable<?> key, boolean right, int level) {
        ArrayList<Link> ret = new ArrayList<Link>(0);
        if (key != null) {
            ChordSharpVNode<E> vnode = (ChordSharpVNode<E>) keyHash.get(key);
            if (vnode == null) {
                return new Link[0];
            }
            FTEntry e = null;
            if (right) { // right
                e = vnode.forwardTable.getFTEntry(level);
            }
            else {
                e = vnode.backwardTable.getFTEntry(level);
            }
            if (e != null) {
                ret.add(e.link);
                if (e.successors != null) {
                    Arrays.stream(e.successors).forEach((link) -> {
                        ret.add(link);
                    });
                }
            }
            return ret.toArray(new Link[0]);
        }
        else {
            keyHash.values().forEach((n) -> {
                ChordSharpVNode<E> vnode = (ChordSharpVNode<E>) n;
                vnode.forwardTable.table.getAll().forEach((e) -> {
                    ret.add(e.link);
                    if (e.successors != null) {
                        Arrays.stream(e.successors).forEach((link) -> {
                            ret.add(link);
                        });
                    }
                });
                vnode.backwardTable.table.getAll().forEach((e) -> {
                    ret.add(e.link);
                    if (e.successors != null) {
                        Arrays.stream(e.successors).forEach((link) -> {
                            ret.add(link);
                        });
                    }
                });
            });
            return ret.toArray(new Link[0]);
        }
    }
    
    public int getHeight(Comparable<?> key) {
        ChordSharpVNode<E> vnode = (ChordSharpVNode<E>) keyHash.get(key);
        return vnode.forwardTable.getFingerTableSize();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<ChordSharpVNode<E>> allVNodes() {
        return (Collection<ChordSharpVNode<E>>) super.allVNodes();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ChordSharpVNode<E>> allValidVNodes() {
        return (List<ChordSharpVNode<E>>) super.allValidVNodes();
    }

    @Override
    public ChordSharpVNode<E> getVNode(Comparable<?> rawkey) {
        return (ChordSharpVNode<E>) super.getVNode(rawkey);
    }

    @Override
    public void fin() {
        super.fin();
        ftPool.shutdown();
    }

    void rqReceiveRequest(ChordSharpRQMessage req) {
        String h = "rqReceiveRequest";
        logger.debug("{}: {}", h, req);
        // 各subrangeの最後のIDを受信しているならば，そのsubrangeは無視する．
        // queryRanges = 残ったsubrangeの集合．
        // TODO: lock queryHistroy, purge entries
        Set<Integer> hist = queryHistory.get(req.qid);
        if (hist == null) {
            hist = new HashSet<Integer>();
            queryHistory.put(req.qid, hist);
        }
        req.unavailableKeys = new ArrayList<DdllKey>();
        Collection<SubRange> queryRanges = new ArrayList<SubRange>();
        for (SubRange range : req.subRanges) {
            // check the specified key's existence
            Link link = range.getLink();
            ChordSharpVNode<?> vn = getVNode(link.key.getPrimaryKey());
            if (vn == null || !vn.getLocalLink().equals(link)) {
                req.unavailableKeys.add(link.key);
                continue;
            }
            // check if we've received the same query
            Integer last = null;
            if (range.ids.length > 0) {
                last = range.ids[range.ids.length - 1];
            }
            assert last != null;
            if (hist.contains(last)) {
                logger.debug("{}: already received (hist={}, req={})", h, hist,
                        req);
                continue;
            } else {
                logger.debug("{}: add {} to history, req={}", h, last, req);
                queryRanges.add(range);
                hist.addAll(Arrays.asList(range.ids));
            }
        }
        logger.debug("{}: qr={}, na={}, hist={}", h, queryRanges,
                req.unavailableKeys, hist);
        if (queryRanges.isEmpty()) {
            return;
        }
        req.subRanges = queryRanges; // overwrite!
        rqDisseminate(req);
    }

    /**
     * get valid finger table entries from all inserted ChordSharpVNode.
     * XXX: this is naive implementation.
     * 
     * @return list of finger table entries
     */
    public List<FTEntry> getValidFTEntries() {
        //logger.debug("getValid: {}", this);
        List<FTEntry> rc = new ArrayList<FTEntry>();
        List<ChordSharpVNode<E>> vnodes = allValidVNodes();
        ChordSharpVNode<E> v1 = vnodes.get(0);
        ChordSharpVNode<E> v2;
        for (int k = 1; k <= vnodes.size(); k++, v1 = v2) {
            v2 = vnodes.get(k % vnodes.size());
            List<FTEntry> flist = new ArrayList<FTEntry>();
            List<FTEntry> blist = new ArrayList<FTEntry>();
            FTEntry me = v1.getLocalFTEnetry();
            flist.add(me);
            FTEntry fent = null;
            FTEntry bent = null;
            int fsz = v1.getFingerTableSize();
            int bsz = v2.getBackwardFingerTableSize();
            // forward ft と backward ft の両方を，0 番目のエントリから順番にスキャンし，
            // 両者が出会うところまで　flist と blist に登録していく．
            for (int i = 0; i < Math.max(fsz, bsz); i++) {
                boolean f = false, b = false;
                if (i < fsz) {
                    fent = v1.getFingerTableEntry(i);
                    f = true;
                }
                if (i < bsz) {
                    bent = v2.getBackwardFingerTableEntry(i);
                    b = true;
                }
                if ((fent != null)
                        && (bent != null)
                        && Node.isOrdered(v1.getKey(), bent.link.key,
                                fent.link.key)) {
                    if (f) {
                        FTEntry bprev;
                        // fentがbprevとbentの間に挟まれるならば，fentを採用
                        // FFT:         F8...F7...F6
                        // BFT: B6...B7....B8 
                        // さもなくば，fentは採用しない
                        // FFT:   F8.........F7...F6
                        // BFT: B6...B7....B8 
                        if (blist.size() > 0) {
                            bprev = blist.get(blist.size() - 1);
                        } else {
                            bprev = me;
                        }
                        if (Node.isOrdered(bent.link.key, fent.link.key,
                                bprev.link.key)) {
                            flist.add(fent);
                        }
                    }
                    break;
                }
                if (f && (fent != null)) {
                    flist.add(fent);
                }
                if (b && (bent != null)) {
                    blist.add(bent);
                }
            }
            Collections.reverse(blist);
            //            logger.debug("getValid: v1={}, v2={}, flist={}, blist={}",
            //                    v1.getKey(), v2.getKey(), flist, blist);
            rc.addAll(flist);
            rc.addAll(blist);
        }
        return rc;
    }

    /**
     * queryRangeを分割する．
     * 分割点は，ftlistで与えられるFTEntryから，queryRangeに含まれるDdllKeyを選ぶ．
     * 
     * @param queryRange
     * @param ftlist
     * @return
     */
    @SuppressWarnings("unused")
    private NavigableMap<DdllKey, FTEntry> fragmentPoints0(
            CircularRange<DdllKey> queryRange, List<FTEntry> ftlist) {
        NavigableMap<DdllKey, FTEntry> frags =
                new ConcurrentSkipListMap<DdllKey, FTEntry>();
        for (FTEntry ent : ftlist) {
            for (Link l : ent.allLinks()) {
                if (statman.isPossiblyFailed(l.addr)) {
                    continue;
                }
                if (queryRange.contains(l.key)) {
                    if (!frags.containsKey(l.key)) {
                        frags.put(l.key, ent);
                    }
                    break;
                }
            }
        }
        logger.debug("fragPoints: QR = {}", queryRange);
        logger.debug("fragPoints: frags = {}", frags);
        return frags;
    }

    /**
     * queryRangeを分割する．
     * 分割点は，ftlistから，queryRange(QR)に含まれ，maybeFailed(MF)に含まれない
     * DdllKeyを選ぶ．
     * ただし，QRに含まれるならば，MFに含まれていてもsuccessorは必ず選ぶ．
     * <p>
     * 返り値は，(分割点, QRに含まれ，MFに含まれない，分割点と等しいか大きいノードの集合)のmap
     * <pre>
     *             [======QR=====]
     *  ftlist:  ABCD           EFGH
     * 上の図の場合，返り値は (C, {C, D}), (E, {E, F}) である．(MFが空のとき)
     * </pre>
     * @param queryRange    query range
     * @param goodNodes        list of list of peers
     * @param closeRanges   list of [my key, successor)
     * @param maybeFailed   nodes not recommended for next hop
     * @return (分割点のDdllKey, 分割点に対応するLinkのList)のNavigableMap
     */
    public NavigableMap<DdllKey, List<Link>> fragmentPoints(
            CircularRange<DdllKey> queryRange, List<List<Link>> goodNodes,
            List<SubRange[]> closeRanges, Set<Endpoint> maybeFailed) {
        NavigableMap<DdllKey, List<Link>> frags =
                new ConcurrentSkipListMap<DdllKey, List<Link>>();

        Set<DdllKey> successors = new HashSet<DdllKey>();
        for (SubRange[] s : closeRanges) {
            successors.add(s[1].to);
        }
        for (List<Link> ent : goodNodes) {
            // entの中からqueryRangeに含まれるノードを抽出したもの
            List<Link> containedLinks = new ArrayList<Link>();
            for (Link l : ent) {
                // maybeFailedに含まれているノードは無視する．
                // ただし successor は除く．
                if (!successors.contains(l.key)) {
                    if (maybeFailed.contains(l.addr)) {
                        continue;
                    }
                    if (unavailableRemoteKeys.contains(l.key)) {
                        continue;
                    }
                }
                if (queryRange.contains(l.key)) {
                    if (!frags.containsKey(l.key)) {
                        frags.put(l.key, containedLinks);
                    }
                    containedLinks.add(l);
                    break;
                }
            }
        }
        logger.debug("fragPoints: QR={}, fragPoints={}", queryRange, frags);
        return frags;
    }

    protected List<Link> filterFailedNodes(FTEntry ftlist) {
        List<Link> ret = new ArrayList<Link>();
        for (Link link : ftlist.allLinks()) {
            if (!statman.isPossiblyFailed(link.addr)) {
                ret.add(link);
            }
        }
        return ret;
    }

    /**
     * split the query range and assign a delegate node to each subrange.
     *  
     * @param queryRange    query range
     * @param allNodes      all nodes in the finger tables
     * @param goodNodes     non-failed nodes in the finger tables
     * @param closeRanges   List of {[predecessor, n), [n, successor}},
     *                      where n is myself.
     * @param maybeFailed   nodes to avoid for next hop if possible
     * @return list of SubRange
     */
    List<SubRange> assignDelegate(SubRange queryRange,
            List<List<Link>> allNodes, List<List<Link>> goodNodes,
            List<SubRange[]> closeRanges, Set<Endpoint> maybeFailed) {
        // queryRangeを分割する．
        NavigableMap<DdllKey, List<Link>> fps =
                fragmentPoints(queryRange, goodNodes, closeRanges, maybeFailed);

        for (SubRange[] s : closeRanges) {
            // make sure that successor is used even if it might be failed 
            SubRange succr = s[1];
            if (queryRange.contains(succr.to)) {
                fps.put(succr.to, Collections.singletonList(succr.getLink()));
                //logger.debug("assignDele: chk0 {}", fps);
            }
        }

        List<SubRange> list = new ArrayList<SubRange>();
        Map.Entry<DdllKey, List<Link>> ent = fps.firstEntry();
        if (ent == null) {
            // queryRangeに含まれるノードが存在しない
            Link dlink =
                    getClosestPredecessor(queryRange.from, goodNodes, allNodes,
                            maybeFailed);
            SubRange dkr = new SubRange(dlink, queryRange, queryRange.ids);
            list.add(dkr);
        } else {
            // queryRangeに含まれるノードが存在する．

            // 以下のような場合，QRから[QR.from, succ)を削除し，meを担当ノードとしておく．
            //      [====QR======]
            //   |    |
            //  me   succ
            for (SubRange[] s : closeRanges) {
                SubRange succr = s[1];
                DdllKey me = succr.from;
                DdllKey succ = succr.to;
                if (!queryRange.contains(me) && queryRange.contains(succ)
                        && queryRange.from.compareTo(succ) != 0) {
                    SubRange dkr =
                            new SubRange(getVNode(me.getPrimaryKey())
                                    .getLocalLink(), queryRange.from,
                                    queryRange.fromInclusive, succ, false);
                    dkr.assignSubId(queryRange);
                    list.add(dkr);
                    //logger.debug("assignDelegate: chk1 {}", dkr);
                    queryRange =
                            new SubRange(null, succ, true, queryRange.to,
                                    queryRange.toInclusive, queryRange.ids);
                }
            }

            boolean first = true;
            while (ent != null) {
                Map.Entry<DdllKey, List<Link>> next =
                        fps.higherEntry(ent.getKey());
                DdllKey from, to;
                boolean fromInclusive, toInclusive;
                if (first) {
                    from = queryRange.from;
                    fromInclusive = queryRange.fromInclusive;
                } else {
                    from = ent.getKey();
                    fromInclusive = true;
                }
                if (next == null) {
                    to = queryRange.to;
                    toInclusive = queryRange.toInclusive;
                } else {
                    to = next.getKey();
                    toInclusive = false;
                }
                // assign a node for delegation
                List<Link> ftent = ent.getValue();
                Link dlink = pickupDelegate(ftent, queryRange, maybeFailed);
                if (first && dlink.key.getUniqId().equals(getPeerId())) {
                    if (from.compareTo(dlink.key) != 0) {
                        // 自ノードが最初のエントリで，さらに左側にQRの領域(***)がある場合
                        // [***=====QR========]
                        //    ^me   ^X

                        // ======]2    8[=====QR==
                        //   ^me (0)
                        // r1 = [8, 0)

                        // ======]2    8[=====QR==
                        //   0             10 
                        Link d =
                                getClosestPredecessor(queryRange.from,
                                        goodNodes, allNodes, maybeFailed);
                        SubRange r1 =
                                new SubRange(d, from, fromInclusive, dlink.key,
                                        false);
                        r1.assignSubId(queryRange);
                        list.add(r1);
                        from = dlink.key;
                        fromInclusive = true;
                        logger.debug("assignDelegate: additional region {}", r1);
                    }
                }
                SubRange dkr =
                        new SubRange(dlink, from, fromInclusive, to,
                                toInclusive);
                dkr.assignSubId(queryRange);
                list.add(dkr);
                ent = next;
                first = false;
            }
        }
        logger.debug("assignDelegate: QR={}, returns {}", queryRange, list);
        return list;
    }

    /**
     * entから，queryRangeに含まれるノードを1つ選ぶ．
     * 選ぶノードは，なるべく maybeFailed に含まれるものは選ばない．
     * 
     * @param ent
     * @param queryRange
     * @param maybeFailed
     * @return
     */
    private Link pickupDelegate(List<Link> ent,
            CircularRange<DdllKey> queryRange, Set<Endpoint> maybeFailed) {
        for (int i = 0; i < 2; i++) {
            for (Link link : ent) {
                if (i == 0 && maybeFailed.contains(link.addr)) {
                    continue;
                }
                if (true || queryRange.contains(link.key)
                        || queryRange.from.compareTo(link.key) == 0) {
                    return link;
                }
            }
        }
        logger.error("no link in {}", queryRange);
        throw new Error("fatal");
    }

    /*
     * key
     *      B2  B1  N  F1  F2
     */
    /*private Link getClosestPredecessorOLD(DdllKey key, List<FTEntry> ftlist) {
        int sz = ftlist.size();
        for (int i = 0; i < sz; i++) {
            FTEntry e1 = ftlist.get(i);
            FTEntry e2 = ftlist.get((i + 1) % sz);
            if (Node.isOrdered(e1.link.key, key, e2.link.key)) {
                logger.debug("getClosestPred: key={}, ftlist={}, ret={}", key,
                        ftlist, e1);
                return e1.link;
            }
        }
        throw new Error("no closest predecessor?");
    }*/

    /**
     * ftlist (非故障ノードリスト) から，指定されたkeyのclosest predecessorを求める．
     * ただし，closest predecesorが自ノードになる場合で，自ノードのsuccessorの方が
     * keyに近い場合(=ftlistにsuccessorが含まれない場合)，successorを返す．
     * 
     * @param key the key
     * @param goodNodes the good nodes
     * @param allNodes the all nodes.
     * @param maybeFailed the nodes that may be failed.
     * @return the closest predecessor.
     */
    protected Link getClosestPredecessor(DdllKey key,
            List<List<Link>> goodNodes, List<List<Link>> allNodes,
            Set<Endpoint> maybeFailed) {
        Link best = getClosestPredecessor0(key, goodNodes, maybeFailed);
        if (best.key.getUniqId().equals(getPeerId())) {
            Link best2 = getClosestPredecessor0(key, allNodes, null);
            logger.debug("getClosestPredecessor: case1: key={}, return {}",
                    key, best2);
            return best2;
        }
        logger.debug("getClosestPredecessor: case2: key={}, return {}", key,
                best);
        return best;
    }

    private Link getClosestPredecessor0(DdllKey key, List<List<Link>> ftlist,
            Set<Endpoint> maybeFailed) {
        Link best = null;
        for (List<Link> list : ftlist) {
            for (Link link : list) {
                if (maybeFailed != null && maybeFailed.contains(link.addr)) {
                    continue;
                }
                if (key.compareTo(link.key) == 0) {
                    return link;
                }
                if (best == null
                        || Node.isOrdered(best.key, false, link.key, key, false)) {
                    best = link;
                }
            }
        }
        return best;
    }

    @Override
    public void rqDisseminate(RQMessage msg, NavigableMap<DdllKey, Link> unused) {
        rtLockW();
        try {
            rqDisseminate0(msg, unused);
        } finally {
            rtUnlockW();
        }
    }

    // XXX: note that wrap-around query ranges such as [10, 5) do not work
    // for now (k-abe)
    private void rqDisseminate0(RQMessage msg,
            NavigableMap<DdllKey, Link> unused) {
        final String h = "rqDiss(id=" + msg.msgId + ")";
        logger.debug("{}: msg = {}", h, msg);
        logger.debug("{}: finger table = {}", h, this);
        logger.debug("{}: node stats = {}", h, statman);

        // collect {[predecessor, me), [me, successor)}
        List<SubRange[]> closeRanges = new ArrayList<SubRange[]>();
        for (ChordSharpVNode<E> vp : allValidVNodes()) {
            Link v = vp.getLocalLink();
            Link succ = vp.getSuccessor();
            Link pred = vp.getPredecessor();
            SubRange l = new SubRange(pred, pred.key, true, v.key, false);
            SubRange r = new SubRange(succ, v.key, true, succ.key, false);
            closeRanges.add(new SubRange[] { l, r });
        }

        // results of locally-resolved ranges
        List<DKRangeRValue<?>> rvals = new ArrayList<DKRangeRValue<?>>();

        RQAlgorithm algo = msg.getRangeQueryAlgorithm();
        StrictMap<Id, List<SubRange>> map =
                algo.assignDelegates(msg, closeRanges, rvals);
        logger.debug("{}: aggregated: {}", h, map);

        /*
         * prepare RQReturn for catching results from children
         */
        if (msg.rqRet == null) {
            msg.rqRet = new RQReturn(this, msg, msg.opts, msg.isRoot);
        }
        RQReturn rqRet = msg.rqRet;
        rqRet.updateHops(msg.hops);

        Collection<MessagePath> paths = new HashSet<MessagePath>();

        /*
         * send the aggregated requests to children.
         */
        for (Map.Entry<Id, List<SubRange>> ent : map.entrySet()) {
            Id p = ent.getKey();
            if (!p.equals(peerId)) {
                List<SubRange> subRanges = ent.getValue();
                logger.debug("{}: forward {}, {}", h, p, subRanges);
                RQMessage m = msg.newChildInstance(subRanges);
                Link l = subRanges.get(0).getLink();
                rqRet.sendChildMessage(l, m);

                if (TransOptions.inspect(msg.opts)) {
                    // generate MessagePath
                    DdllKey from = keyHash.firstEntry().getValue().getKey();
                    // 送信側で MessagePath を生成しているのでホップ数は +1 している
                    MessagePath mp =
                            new MessagePath(msg.hops + 1, from, l.key,
                                    subRanges);
                    logger.debug("mp={}", mp);
                    paths.add(mp);
                }
            }
        }

        /*
         * execute range query for locally resolvable ranges. 
         */
        algo.rqExecuteLocal(msg, map.get(peerId), rvals);

        logger.debug("rqDisseminate: rvals = {}", rvals);

        /*
         * store the results of execQuery into rqRet.
         * note that addRemoteValue() may send a reply internally.
         */
        if (TransOptions.inspect(msg.opts)) {
            rqRet.addMessagePaths(paths);
        }
        rqRet.addRemoteValues(rvals);
        ResponseType rtype = TransOptions.responseType(msg.opts);
        if (((rtype == ResponseType.DIRECT) && !msg.isRoot)
                || (rtype == ResponseType.NO_RESPONSE)) {
            rqRet.flush();
            rqRet.dispose();
        }
        logger.debug("rqDisseminate finished");
    }

    /**
     * 各 range を subRange に分割し，それぞれ担当ノードを割り当てる．
     * 各ノード毎に割り当てたSubRangeのリストのMapを返す．
     * 
     * @param msg the message for range query.
     * @param closeRanges   List of {[predecessor, n), [n, successor}},
     *                      where n is myself.
     * @return a map of id and subranges.
     */
    protected StrictMap<Id, List<SubRange>> assignDelegates(RQMessage msg,
            List<SubRange[]> closeRanges) {
        StrictMap<Id, List<SubRange>> map =
                new StrictMap<Id, List<SubRange>>(
                        new HashMap<Id, List<SubRange>>());
        Set<Endpoint> maybeFailed = msg.failedLinks;
        // make sure that i'm not failed 
        maybeFailed.remove(manager.getEndpoint());

        List<FTEntry> ftlist = getValidFTEntries();
        List<List<Link>> allNodes = new ArrayList<List<Link>>();
        List<List<Link>> goodNodes = new ArrayList<List<Link>>();
        for (FTEntry ftent : ftlist) {
            allNodes.add(ftent.allLinks());
            List<Link> links = filterFailedNodes(ftent);
            goodNodes.add(links);
        }
        logger.debug("allNodes={}, goodNodes={}, maybeFailed={}", allNodes,
                goodNodes, maybeFailed);
        logger.debug("subRanges={}", msg.subRanges);
        for (SubRange range : msg.subRanges) {
            List<SubRange> dkrlist =
                    assignDelegate(range, allNodes, goodNodes, closeRanges,
                            maybeFailed);
            aggregateDelegates(dkrlist, map);
        }
        return map;
    }

    public void aggregateDelegates(List<SubRange> dkrlist,
            StrictMap<Id, List<SubRange>> map) {
        for (SubRange dkr : dkrlist) {
            UniqId id = dkr.getLink().key.getUniqId();
            List<SubRange> list = map.get(id);
            if (list == null) {
                list = new ArrayList<SubRange>();
                map.put(id, list);
            }
            list.add(dkr);
        }
    }

    public void scheduleFingerTableUpdate(int delay, int interval) {
        for (ChordSharpVNode<E> node : allVNodes()) {
            node.scheduleFTUpdate(delay, interval);
        }
    }
}
