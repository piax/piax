/*
 * RQManager.java - A node of range queriable overlay.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.ring.rq;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.piax.common.Endpoint;
import org.piax.common.Id;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.common.subspace.CircularRange;
import org.piax.common.subspace.Range;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.DeliveryMode;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.Node;
import org.piax.gtrans.ov.ddll.Node.InsertPoint;
import org.piax.gtrans.ov.ddll.Node.Mode;
import org.piax.gtrans.ov.ddll.NodeManagerIf;
import org.piax.gtrans.ov.ring.NoSuchKeyException;
import org.piax.gtrans.ov.ring.RingManager;
import org.piax.gtrans.ov.ring.RingVNode;
import org.piax.gtrans.ov.ring.RingVNode.VNodeMode;
import org.piax.gtrans.ov.ring.TemporaryIOException;
import org.piax.gtrans.ov.ring.UnavailableException;
import org.piax.gtrans.ov.sg.SkipGraph;
import org.piax.util.StrictMap;
import org.piax.util.UniqId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * - Usual Case
 *
 *                   +------------------------+
 *                   |  RQMessage (received)  |
 *                   +------------------------+
 *                        |rqRet        ^
 *                        v             |parentMsg
 * FrameWork         +------------------------+[rqDisseminate]
 * +--------+        |       RQReturn         |
 * |msgStore|        +------------------------+
 * |        |             |childMsgs*   ^
 * |        |             v             |rqRet
 * |        |  id*   +------------------------+[RQMessage#newChildInstance]
 * |        |------->|  RQMessage (to child)  |
 * +--------+        +------------------------+
 *
 *
 * - Fast Retransmission
 *
 * Fast Retransmission では，ACKがタイムアウトした RQMessage (to child) を指定して
 * rqDisseminate() を呼ぶ．再送で送られるRQMessageは，RQReturnのchildMsgsに追加される．
 * 再送した後で最初の RQMessage に対する応答が到着する可能性があるため，
 * RQMessage (to child 1) は削除しない．
 * 削除は，RQReturn#dispose が呼ばれた際に行う．
 * 
 *                   +------------------------+
 *                   |  RQMessage (received)  |
 *                   +------------------------+
 *                        |rqRet        ^
 *                        v             |parentMsg
 * FrameWork         +------------------------+
 * +--------+        |       RQReturn         | [rqDisseminate]
 * |msgStore|        +------------------------+
 * |        |           |childMsgs ^    ^
 * |        |           |          |    |rqRet
 * |        |  id*      |          |  +---------------------+[RQMessage#newChildInstance]
 * |        |----------)|(--------)|(>|RQMessage(to child 1)| (timed-out instance)
 * |        |           |          |  +---------------------+
 * |        |           v          |rqRet
 * |        |  id*   +----------------------+
 * |        |------->|RQMessage (to child 2)| (retransmit instance)
 * +--------+        +----------------------+
 *
 * queryがexpireしたら，RQReturn#disposeが呼ばれるので，ここでchildMsgsからRQMessageを削除する．
 *
 *
 * - Slow Retransmission Case
 *
 *                   +------------------------+
 *                   |  RQMessage (received)  |
 *                   +------------------------+
 *                        |rqRet ^
 *                        |      |    +-------------------+ [RQReturn#retransmit()]
 *                        |      |    |RQMessage (retrans)|
 *                        |      |    +-------------------+
 *                        |      |           |rqRet
 *                        v      |parentMsg  v
 * FrameWork         +------------------------+
 * +--------+        |       RQReturn         | [rqDisseminate]
 * |msgStore|        +------------------------+
 * |        |             |childMsgs*   ^
 * |        |             v             |rqRet
 * |        |  id*   +------------------------+ [RQMessage#newChildInstance]
 * |        |------->|  RQMessage (to child)  |
 * +--------+        +------------------------+
 *
 * RQMessage (retrans) では，gap 部分だけが subranges に指定されている．
 */

/**
 * this class adds range query functionality to the Ring network.
 * 
 * @param <E> the type of Endpoint in the underlying network.
 * 
 */
public class RQManager<E extends Endpoint> extends RingManager<E> implements
        RQIf<E> {
    /*--- logger ---*/
    static final Logger logger = LoggerFactory.getLogger(RQManager.class);

    /*
     * every QID_EXPIRETION_TASK_PERIOD, all QueryId entries older than
     * QID_EXPIRE milliseconds are removed. 
     */
    /** expiration time for purging stale QueryIDs */
    public static int QID_EXPIRE = 120 * 1000; // 2min
    /** period for executing a task for purging stale QueryIDs */
    public static int QID_EXPIRATION_TASK_PERIOD = 20 * 1000; // 20sec

    /**
     * Range Queryでトラバース中に通信エラーが起きた場合に戻れるノード数
     */
    public static int RQ_NRECENT = 10;

    /** the period for flushing partial results in intermediate nodes */
    public static int RQ_FLUSH_PERIOD = 2000;
    /** additional grace time before removing RQReturn in intermediate nodes */
    public static int RQ_EXPIRATION_GRACE = 5 * 1000;
    /** range query retransmission period */
    public static int RQ_RETRANS_PERIOD = 10 * 1000;
    /** used as the query string for finding insert points.
     * see {@link SkipGraph#find(Endpoint, DdllKey, boolean)} */
    public final static String QUERY_INSERT_POINT_SPECIAL =
            "*InsertPointSpecial*";
    public final static String QUERY_KEY_SPECIAL = "*QueryKeySpecial*";
    public final static ObjectId RQ_QUERY_AT_FIND = new ObjectId("*QueryAtFind*");

    /** timeout for {@link #find(Endpoint, DdllKey, boolean, Object, TransOptions)} */
    public static int FIND_INSERT_POINT_TIMEOUT = 30 * 1000;

    /** pseudo PeerID used by {@link #rqDisseminate(RQMessage, NavigableMap)} */
    protected final static UniqId FIXPEERID = UniqId.PLUS_INFINITY;
    /** pseudo Link instance that represents the link should be fixed */
    public/*protected*/final Link FIXLEFT;

    public final static boolean NEWALGORITHM = true;

    protected final RQExecQueryCallback execQueryCallback;
    // the algorithm used for locating insertion positions 
    private RQAlgorithm stdRQAlgo;
    
    public RQManager(TransportId transId, ChannelTransport<E> trans,
            RQExecQueryCallback execQueryCallback)
                    throws IdConflictException, IOException {
        super(transId, trans);
        this.execQueryCallback = execQueryCallback;
        FIXLEFT = new Link(myLocator, new DdllKey(0, FIXPEERID));

        schedule(new PurgeTask(),
                (long) (Math.random() * QID_EXPIRATION_TASK_PERIOD),
                QID_EXPIRATION_TASK_PERIOD);
    }

    protected boolean preferDelegateNodeLeftSide() {
        return true;
    }

    @Override
    public RQVNode<E> getVNode(Comparable<?> rawkey) {
        return (RQVNode<E>) keyHash.get(rawkey);
    }

    protected void setRQAlgorithm(RQAlgorithm algo) {
        this.stdRQAlgo = algo;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RQIf<E> getStub(E addr, int rpcTimeout) {
        return (RQIf<E>) super.getStub(addr, rpcTimeout);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RQIf<E> getStub(Endpoint dst) {
        return (RQIf<E>) super.getStub(dst);
    }

    /**
     * find a location to insert `key'.
     * 
     * @param introducer
     *            the node to communicate with.
     * @param key
     *            the query key
     * @param query the object for query.
     * @param opts the transport options.
     * @return the insertion point for `key'
     * @throws UnavailableException
     *             自ノードあるいはseedにkeyが登録されていない
     * @throws IOException
     *             communication error
     */
    @Override
    public InsertPoint findImmedNeighbors(E introducer, DdllKey key, Object query, TransOptions opts)
            throws UnavailableException, IOException {
        logger.debug("introducer={}, key={}", introducer, key);
        NavigableMap<DdllKey, Link> links;
        if (introducer == null) {
            // use the local routing table
            rtLockR();
            links = getAvailableLinks();
            rtUnlockR();
            if (links.size() == 0) {
                return null;
                //throw new UnavailableException(
                //        "no key is available at local node");
            }
        } else { // ask the introducer
            RQIf<E> stub = getStub(introducer);
            if (!NEWALGORITHM) {
                Link[] remoteLinks;
                try {
                    remoteLinks = stub.getLocalLinks();
                } catch (RPCException e) {
                    logger.debug("", e);
                    if (e.getCause() instanceof IOException) {
                        throw (IOException) e.getCause();
                    }
                    throw new IOException(e.getCause());
                }
                if (remoteLinks.length == 0) {
                    throw new UnavailableException(
                            "no key is available at remote node: " + introducer);
                }
                logger.debug("find: remoteLinks = {}",
                        Arrays.toString(remoteLinks));
                links = new ConcurrentSkipListMap<DdllKey, Link>();
                links.put(remoteLinks[0].key, remoteLinks[0]);
            } else { // NEWALGORITHM
                InsertPoint ip;
                try {
                    ip = stub.findImmedNeighbors(null, key, query, opts);
                } catch (RPCException e) {
                    logger.debug("", e);
                    if (e.getCause() instanceof IOException) {
                        throw (IOException) e.getCause();
                    }
                    throw new IOException(e.getCause());
                }
                return ip;
            }
        }

        SubRange range = new SubRange(key, true, key, true);
        TransOptions newOpts;
        if (opts == null) {
            newOpts = new TransOptions(FIND_INSERT_POINT_TIMEOUT, ResponseType.DIRECT);
        }
        else {
            newOpts = opts;
        }
        long timeout = newOpts.getTimeout();
        RQReturn rqRet =
                rqStartKeyRange(Collections.<SubRange> singleton(range),
                        query == null ? QUERY_INSERT_POINT_SPECIAL : query,
                        newOpts,
                        RQ_RETRANS_PERIOD, links, stdRQAlgo);
        logger.debug("find: waiting {}", rqRet);
        try {
            Collection<RemoteValue<?>> col = rqRet.get(timeout);
            // the case where the key has been inserted
            logger.debug("find: col = {}", col);
        } catch (InterruptedException e) {
            throw new IOException("range query timeout");
        }
        logger.debug("rqRet = {}", rqRet);
        // 本来，col から insp を取得すべきだが，col には aux 情報がないので，
        // 仕方なく rqRet.rvals から直接取得している．
        for (DKRangeRValue<?> kr : rqRet.rvals.values()) {
            if (kr.getRemoteValue().getOption() != null) {
                InsertPoint insp = (InsertPoint) kr.getRemoteValue().getOption();
                logger.debug("find: insert point = {}, {}", insp, kr);
                return insp;
            }
        }
        if (TransOptions.responseType(opts) == ResponseType.NO_RESPONSE) {
        		return null;
        }
        /*
         * FIND_INSERT_POINT_TIMEOUT 内に挿入位置が得られなかった．
         * skip graph を修復中などの場合，ここに到達する可能性がある．
         * 呼び出し側でリトライさせるために TemporaryIOException をスローする．
         */
        throw new TemporaryIOException("could not find insert point @" + getEndpoint() + " for " + key);
    }

    // professional version
    public RQResults scalableRangeQueryPro(
            Collection<? extends Range<?>> ranges, Object query, TransOptions opts) {
        if (ranges.size() == 0) {
            return new RQResults();
        }
        RQReturn rqRet =
                rqStartRawRange(ranges, query, opts, RQ_RETRANS_PERIOD, null, stdRQAlgo);
        return rqRet.results;
    }

    /**
     * perform a range query (internal).
     * 
     * @param ranges
     *            ranges for the range query
     * @param query
     *            the query object
     * @param opts
     *            transmission option
     * @param retransPeriod
     *            slow retransmission period (in msec)
     * @param allLinks
     *            all links to split the ranges.
     * @param rqAlgo
     *            range query algorithm
     * @return RQReturn
     */
    protected RQReturn rqStartRawRange(Collection<? extends Range<?>> ranges,
            Object query, TransOptions opts, int retransPeriod,
            NavigableMap<DdllKey, Link> allLinks,
            RQAlgorithm rqAlgo) {
        // convert ranges of Comparable<?> into ranges of <DdllKey>.
        Collection<SubRange> subRanges = new ArrayList<SubRange>();
        for (Range<? extends Comparable<?>> range : ranges) {
            SubRange keyRange = convertToSubRange(range);
            keyRange.assignId(); // root id
            subRanges.add(keyRange);
        }
        return rqStartKeyRange(subRanges, query, opts, retransPeriod,
               allLinks, rqAlgo);
    }

    public static SubRange convertToSubRange(
            Range<? extends Comparable<?>> range) {
        SubRange keyRange =
                new SubRange(
                        new DdllKey(range.from, range.fromInclusive
                                ? UniqId.MINUS_INFINITY : UniqId.PLUS_INFINITY),
                        true,
                        new DdllKey(range.to, range.toInclusive
                                ? UniqId.PLUS_INFINITY : UniqId.MINUS_INFINITY),
                        false);
        return keyRange;
    }

    private RQReturn rqStartKeyRange(Collection<SubRange> ranges, Object query,
            TransOptions opts, int retransPeriod,
            NavigableMap<DdllKey, Link> allLinks, RQAlgorithm rqAlgo) {
        QueryId qid = new QueryId(peerId, rand.nextLong());
        if (opts == null) {
            opts = new TransOptions();  // use default
        }
        RQMessage msg = rqAlgo.newRQMessage4Root(msgframe, ranges, qid, query,
                opts);
        rqDisseminate(msg, allLinks);
        return msg.rqRet;
    }

    public void rqDisseminate(RQMessage msg) {
        rqDisseminate(msg, null);
    }

    // this method is overridden by ChordSharp
    public void rqDisseminate(RQMessage msg,
            NavigableMap<DdllKey, Link> allLinks) {
        rtLockW();
        try {
            rqDisseminate0(msg, allLinks);
        } finally {
            rtUnlockW();
        }
    }

    /* 
     * レンジクエリの考え方:
     *
     * 10----------------->60------------->100
     * 10--------->40----->60----->80----->100
     * 10->20->30->40->50->60->70->80->90->100
     * 
     * Node 10 から [30, 70] で range query した場合:
     * 
     * 10の動作:
     * 40に RQMessage [30, 60),
     * 60に RQMessage [60, 70]を送信 
     * 
     * 40 ([30, 60) を受信している) の動作:
     * 30に RQMessage [30, 40),
     * 50に RQMessage [50, 60)を送信
     * 
     * 左端の範囲は注意が必要である． 
     * 30が受信した [30, 40) は，実際は ((30, -infinity), (40, +infinity))]
     * と解釈する．((-30, -infinity), (30, ???)) の範囲が残るため，
     * (-30, -infinity)の左ノード(ここでは20)にクエリを転送する．
     * 20と，20のLevel 0の右リンク(30)によって 
     * ((-30, -infinity), (30, ???)) の範囲はカバーされるため，この範囲は処理済み
     * として消去する．
     */
    private void rqDisseminate0(RQMessage msg,
            NavigableMap<DdllKey, Link> allLinks) {
        final String h = "rqDiss(id=" + msg.msgId + ")";
        logger.debug("{}: msg = {}", h, msg);

        if (allLinks == null) {
            //rtLockR();
            allLinks = getAvailableLinks();
            //rtUnlockR();
        }
        if (allLinks.isEmpty()) {
            // 挿入が完了していないノードにクエリが転送された．
            // （強制終了してすぐに再挿入した場合も，そのことを知らない
            // ノードからクエリが転送される可能性がある）．
            // ここでは単にクエリを無視することにする．
            logger.warn("routing table is empty!: {}", this);
            return;
        }

        /*
         * split ranges into subranges and assign a delegate node for them.
         * also aggregate each subranges by destination peerIds.
         */
        StrictMap<Id, List<SubRange>> map =
                new StrictMap<Id, List<SubRange>>(
                        new HashMap<Id, List<SubRange>>());
        List<DKRangeRValue<?>> rvals =
                new ArrayList<DKRangeRValue<?>>();
        for (SubRange subRange : msg.subRanges) {
            List<SubRange> subsubRanges =
                    rqSplit(msg.query, subRange, allLinks, msg.failedLinks,
                            rvals, msg.getRangeQueryAlgorithm());
            if (subsubRanges == null) {
                continue;
            }
            logger.debug("subsubRanges = {}", subsubRanges);

            for (SubRange kr : subsubRanges) {
                UniqId pid =
                        (kr.getLink() == FIXLEFT ? FIXPEERID
                                : kr.getLink().key.getUniqId());
                List<SubRange> list = map.get(pid);
                if (list == null) {
                    list = new ArrayList<SubRange>();
                    map.put(pid, list);
                }
                list.add(kr);
            }
        }
        logger.debug("{}: aggregated: {}", h, map);
        logger.debug("{}: msg = {}", h, msg.toString());
        /*
         * prepare RQReturn for catching results from children
         */
        if (msg.rqRet == null) {
            msg.rqRet = new RQReturn(this, msg, msg.opts, msg.isRoot);
        }
        RQReturn rqRet = msg.rqRet;
        if (TransOptions.inspect(msg.opts)) {
        		rqRet.updateHops(msg.hops);
        }

        Collection<MessagePath> paths = new HashSet<MessagePath>();
        /*
         * send the aggregated requests to children.
         * also gather failed ranges that should be retransmit.
         * 
         * 担当ノードが FIXLEFT (FIXPEERID) の範囲を failedRanges に集める．
         */
        List<CircularRange<DdllKey>> failedRanges =
                new ArrayList<CircularRange<DdllKey>>();
        //synchronized (rqRet) {
            for (Map.Entry<Id, List<SubRange>> ent : map.entrySet()) {
                Id p = ent.getKey();
                if (p.equals(FIXPEERID)) {
                    failedRanges.addAll(ent.getValue());
                } else if (!p.equals(peerId)) {
                    List<SubRange> subRanges = ent.getValue();
                    logger.debug("{}: forward {}, {}", h, p, subRanges);
                    RQMessage m = msg.newChildInstance(subRanges);
                    Link l = subRanges.get(0).getLink();
                    /*rqRet.childMsgs.put(l, m);
                    m.send(l);*/
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
        //}
        // N----------C
        // N------B---C
        // N-->A--B---C
        // RQ [r1)[r2)
        //    [--r3--)
        // A, Bが故障している場合，failedRanges には r1 と r2 が入る．
        // これをマージする(r3)．この処理は必須ではない．
        failedRanges = RangeUtils.concatAdjacentRanges(failedRanges);
        logger.debug(h + ": merged failedRanges = " + failedRanges);

        /*
         * fix the local routing table
         * XXX: think!
         */
        if (!msg.failedLinks.isEmpty()) {
            /*rtLockR();
            try {
                for (RingVNode<E> sgnode : keyHash.values()) {
                    for (Endpoint link : msg.failedLinks) {
                        sgnode.fixLeftLinks(link, msg.failedLinks, msg,
                                failedRanges);
                    }
                }
            } finally {
                rtUnlockR();
            }*/
        }

        /*
         * execute range query for locally resolvable range. 
         */
        msg.getRangeQueryAlgorithm()
                .rqExecuteLocal(msg, map.get(peerId), rvals);

        logger.debug("rqDisseminate: rvals = {}", rvals);

        /*
         * store the results of execQuery into rqRet.
         * note that addRemoteValue() may send a reply internally.
         */
        //synchronized (rqRet) {
            if (TransOptions.inspect(msg.opts)) {
                rqRet.addMessagePaths(paths);
            }
            rqRet.addRemoteValues(rvals);
        //}
        //if ((TransOptions.responseType(msg.opts) == ResponseType.DIRECT) && !msg.isRoot) {
        ResponseType rtype = TransOptions.responseType(msg.opts);  
        if (((rtype == ResponseType.DIRECT) && !msg.isRoot) ||
        		(rtype == ResponseType.NO_RESPONSE)) {
            rqRet.flush();
            rqRet.dispose();
        }
        logger.debug("rqDisseminate finished");
    }

    /**
     * Split a range into subranges, by the keys in allLinks. Also assign an
     * appropriate remote delegate node for each subrange.
     * 
     * @param query the query object.
     * @param range0    the range to be split
     * @param allLinks all links to split ranges.
     * @param failedLinks the failed links.
     * @param rvals return values for each range.
     * @param rqAlgo the algorithm for the range query.
     * @return the list of subranges. 
     */
    protected List<SubRange> rqSplit(Object query, final SubRange range0,
            final NavigableMap<DdllKey, Link> allLinks,
            Collection<Endpoint> failedLinks,
            List<DKRangeRValue<?>> rvals, RQAlgorithm rqAlgo) {
        String h = "rqSplit";

        if (failedLinks == null) {
            failedLinks = Collections.emptySet();
        }
        /*
         * failedLinkを作成した時点と、ここに到達した時は、状況が異なるので
         * 自身がfailedLinkに含まれることはありうる
         */
        for (Iterator<Endpoint> fi = failedLinks.iterator(); fi.hasNext();) {
            Endpoint f = fi.next();
            // 自身ならば削除する
            if (getEndpoint().equals(f)) {
                fi.remove();
            }
            /*if (f.key.getUniqId().equals(new UniqId(peerId))) {
                failedLinks.remove(f);
            }*/
        }

        // Range       [---------------] に対し，
        //          N-----A (L0)
        // のように，L0において自ノード(N)と右ノード(A)の間にrangeの左端が入る場合，rangeを
        // 以下のように縮小する．
        // Range ......   [------------]
        // Shrunk      [--)
        // 縮小された区間(Shrunk)には，値が存在しない．これを示すため，ダミーの値nullを
        // rvalsに追加する．
        SubRange range = range0;
        rtLockR();
        try {
            for (RingVNode<E> node : keyHash.values()) {
                if (node.getMode() != VNodeMode.INSERTED) {
                    continue;
                }
                /*Tile t = node.getTile(0);
                if (t == null || t.mode != LvState.INSERTED) {
                    continue;
                }*/
                Link right = node.getSuccessor();
                logger.debug("{}, node = {}, range = {}, right = {}", h, node,
                        range, right);
                assert right != null;
                if (!range.contains(node.getKey())) {
                    // add the information about the shrunk area to the rvals.
                    // this information is essential for the requesting peer to
                    // determine whether all results have been received.
                    Range<DdllKey> removed =
                            RangeUtils.removedRange(range, node.getKey(),
                                    right.key);
                    if (removed == null) {
                        continue;
                    }
                    // add a dummy value for the shrunk range
                    RemoteValue<Object> rv = new RemoteValue<Object>(peerId);
                    InsertPoint insp =
                            new InsertPoint(node.getLocalLink(), right);
                    rv.setOption(insp);
                    DKRangeRValue<?> kr =
                            new DKRangeRValue<Object>(rv, removed);
                    logger.debug("{}: dummy rval {}", h, kr);
                    rvals.add(kr);
                    // shrink the range
                    /*range =
                            RangeUtils.retainRange(range, node.getKey(),
                                    right.key);*/
                    //range = range.retainRange(node.getKey(), right.key);
                    SubRange[] ranges = range.retainRanges(node.getKey(), right.key);
                    logger.debug("{}: retain = {}", h, ranges);
                    if (ranges == null) {
                        return null;
                    }
                    range = ranges[0];  // XXX: think!
                }
            }
        } finally {
            rtUnlockR();
        }
        if (range0 != range) {
            logger.debug("{}: shrunk, {} => {}", h, range0, range);
        }

        // Split the given range into subranges by all the keys in my routing
        // table and failedLinks
        //
        // Range [------------------]
        // Keys _____x____x_____x____
        // Result [--)[---)[----)[---]

        // extAllLinks = allLinks + failedLinks
        List<SubRange> ranges =
                rqAlgo.assignDelegate(query, range, allLinks, failedLinks);

        return ranges;
    }

    /*
     * End of scalable range queries
     */

    /**
     * 指定されたkeyとNavigableCondの条件を満たすキーを持つピア（最大num個）にクエリを転送し、
     * 得られた結果（リスト）を返す。
     * <p>
     * 例えば、keyが10、NavigableCondがLOWER、numが5の場合、10を越えなくて、
     * 10の近傍にある最大5個のキーが探索対象となる。 numに2以上をセットする用途としては、
     * DHTのように特定のkeyの近傍集合に
     * アクセスするケースがある。 尚、探索対象となるキーは、指定されたkeyと同じ型を持つ必要が
     * ある。
     * 
     * @param isPlusDir +方向にrangeにサーチする場合はtrue
     * @param range 探索対象となるRange
     * @param maxNum 条件を満たす最大個数
     * @param query the query passed to execQuery()
     * @param opts the transport options.
     * @return クエリ結果のリスト
     */
    public List<RemoteValue<?>> forwardQuery(boolean isPlusDir, Range<?> range,
            int maxNum, Object query, TransOptions opts) {
        logger.trace("ENTRY:");
        logger.debug("isPlusdir:{}, range:{}, num:{}", isPlusDir, range, maxNum);
        try {
            if (!isPlusDir) {
                // lower
                return forwardQueryLeft(range, maxNum, query, opts, false);
            } else {
                throw new UnsupportedOperationException(
                        "upper is not supported");
            }
        } finally {
            logger.trace("EXIT:");
        }
    }
    
    /**
     * 
     * @param range 探索対象となるRange
     * @param maxNum 条件を満たす最大個数
     * @param query the query passed to execQuery()
     * @param opts the TransOptions
     * @param wrapAround
     * @return クエリ結果のリスト
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<RemoteValue<?>> forwardQueryLeft(Range<?> range, int num,
            Object query, TransOptions opts, boolean wrapAround) {
        Comparable rawFromKey = range.to;
        DdllKey fromKey =
                range.toInclusive ? new DdllKey(rawFromKey,
                        UniqId.PLUS_INFINITY) : new DdllKey(rawFromKey,
                        UniqId.MINUS_INFINITY);
        Comparable rawToKey = range.from;
        DdllKey toKey =
                range.fromInclusive ? new DdllKey(rawToKey,
                        UniqId.MINUS_INFINITY) : new DdllKey(rawToKey,
                        UniqId.PLUS_INFINITY);

        List<RemoteValue<?>> rset = new ArrayList<RemoteValue<?>>();
        QueryId qid = new QueryId(peerId, rand.nextLong());
        // n1 -> n2 -> n3 と辿って，n3と通信できなかった場合，再度n2と通信して
        // n2の右リンクが修復されるのを待つ．n2とも通信できなくなると，n1に戻る．
        // このために，直前に辿った RQ_NRECENT 個のノードを保持する．
        LinkedList<Link> trace = new LinkedList<Link>();
        // nから左方向に，レベル0のリンクを使ってトラバースする．
        boolean getStartNode = true;
        Link n = null, nRight = null;
        while (true) {
            if (getStartNode) {
                // get the start node
                try {
                		logger.debug("num={}, opts={}, query={}", num, opts, query);
                		// XXX Only the NO_RESPONSE case is treated here.
                		// execute the query at the receiver.
                		InsertPoint links = find(null, fromKey, true,
                    		(num == 1 && TransOptions.responseType(opts) == ResponseType.NO_RESPONSE) ?
                    		new NestedMessage(RQ_QUERY_AT_FIND, 
                    				RQ_QUERY_AT_FIND, peerId, myLocator, query) : null, 
                    		opts);
                    // XXX find returns null if NO_RESPONSE.
                    if (links == null) return null;
                    n = links.left;
                    nRight = links.right;
                } catch (UnavailableException e) {
                    // offline?
                    logger.error("", e);
                    return null;
                } catch (IOException e) {
                    // should not happen
                    logger.error("", e);
                    return null;
                }
                getStartNode = false;
            }
            if (!new Range(toKey, fromKey).contains(n.key)) {
                logger.debug("forwardQueryLeft: finish (reached end of range) [{} {}], {}",toKey,fromKey,n.key);
                break;
            }
            // 例えば key 0 しか存在しないときに，find() で -1 を検索すると，一周して
            // key 0 が帰ってくる．この場合はクエリ対象が存在しないので終了する．
            if (!wrapAround
                    && keyComp.compare(rawFromKey, n.key.getPrimaryKey()) < 0) {
                logger.debug("forwardQueryLeft: finish (no node is smaller than rawFromKey)");
                break;
            }
            logger.debug("forwardQueryLeft: query to " + n);
            // ここのstubではexecQueryを呼び出すため、デフォルトのタイムアウト値を用いる必要がある
            RQIf stub = getStub((E) n.addr);
            ExecQueryReturn eqr;
            // doActionフラグは，Queryを受信したノードで対応するアクションを
            // 実行するかどうかを決定する．
            boolean doAction = true;
            try {
                eqr = stub.invokeExecQuery(n.key.getPrimaryKey(), nRight,
                		  qid, doAction, query, opts);
            } catch (RightNodeMismatch e) {
                // XXX: not tested
                logger.debug("", e);
                if (keyComp.isOrdered(n.key, e.curRight.key, nRight.key)) {
                    // ---> X
                    // N <------- nRight
                    // おそらくnRightがXからのSetLをまだ受信していない．
                    // N := X としてやりなおす
                    n = e.curRight;
                    logger.debug("forwardQueryLeft: right node mismatch. "
                            + "restart from {}", n);
                    continue;
                }
                // --------------> X
                // N <--- nRight
                // Nの右ノードがnRightを行き過ぎている．以下の可能性がある．
                // (1) nRightは削除された．
                // (2) XがnRightを誤って故障したと判断し，Nの右リンクをXに強制的に付け替え．
                // とりあえず，そのまま処理を継続することにする．
                nRight = e.curRight;
                logger.debug("forwardQueryLeft: right node mismatch. "
                        + "continue from {}", n);
                continue;
            } catch (Throwable e) {
                assert e instanceof UnavailableException
                        || e instanceof RPCException;
                Throwable cause =
                        (e instanceof RPCException ? e.getCause() : e);
                if (!(cause instanceof UnavailableException || cause instanceof InterruptedIOException)) {
                    // execQuery() をRPCで処理した際に起こるstub回りタイムアウトを除く例外と
                    // execQuery() の外で起こる invokeExecQuery() における例外のケース
                    // バグとして扱う．
                    logger.error("forwardQueryLeft: got {}"
                            + " when calling invokeExecQuery() on {}", cause, n);
                    break;
                }
                logger.debug("", cause);
                if (trace.size() == 0) {
                    logger.debug("forwardQueryLeft: start over");
                    getStartNode = true;
                } else {
                    // nextは最後に通信したノード
                    Link next = trace.removeLast();
                    logger.debug("forwardQueryLeft: retry from {}", next);
                    // nextに，左リンクの修復を要求する
                    // TODO: nextの複数のレベルで左リンクがnの可能性があるため，
                    // 本来はNodeではなくSkipGraphのレベルで左リンクの修復を要求するべき．
                    NodeManagerIf stub2 = manager.getStub(next.addr);
                    try {
                        stub2.startFix(next.key, n, false);
                    } catch (Exception e2) {
                        logger.info("", e2);
                    }
                    // nextで左リンクを修復するためには，getStatがタイムアウトする時間
                    // が必要なので，その時間待つ．
                    try {
                        // TODO checked by yos. possibility of hardcoding
                        Thread.sleep(Node.GETSTAT_OP_TIMEOUT + 100);
                    } catch (InterruptedException e2) {
                    }
                    n = next;
                    nRight = null; // XXX: Think!!!
                }
                continue;
            }
            // eqr.key == null if the message is retransmitted
            if (doAction && eqr.key != null) {
                rset.add(eqr.key);
            }
            if (rset.size() >= num) {
                logger.debug("forwardQueryLeft: got enough");
                break;
            }
            // 一周したかチェック
            // XXX: 左リンクが正しいものとして扱っている
            if (Node.isOrdered(eqr.left.key, fromKey, n.key)) {
                logger.debug("forwardQueryLeft: circulated");
                break;
            }
            // n の左ノードが n と等しい場合，可能性としては
            // (1)n の SetL の受信が遅れている (2)nしか存在しない，の2通り．
            // (1)の場合，nの右ノードはnを指さないが，(2)の場合はnを指す．
            // (1)の場合はnに再送，(2)の場合は終了する．
            if (n.key.equals(eqr.left.key) && n.key.equals(eqr.right.key)) {
                logger.debug("forwardQueryLeft: just single node exists");
                break;
            }
            if (!n.equals(eqr.left)) {
                trace.addLast(n);
                nRight = n;
                n = eqr.left;
                if (trace.size() > RQ_NRECENT) {
                    trace.removeFirst();
                }
                logger.debug("trace= {}", trace);
            }
        }
        return rset;
    }

    /**
     * Range Query実行時にRPCで呼び出されるメソッド．
     * 指定されたパラメータで execQuery を実行する． 
     * curRight != null の場合，レベル0の右リンクがcurRightと等しい場合にのみ実行．
     * execQueryの結果(RemoteValue)と，レベル0での左右のリンクを
     * ExecQueryReturn に詰めて返す．
     * 
     * @throws NoSuchKeyException
     *             rawkeyが存在しない
     * @throws RightNodeMismatch
     *             curRightがマッチしない
     */
    public ExecQueryReturn invokeExecQuery(Comparable<?> rawkey, Link curRight,
            QueryId qid, boolean doAction, Object query, TransOptions opts)
            throws NoSuchKeyException, RightNodeMismatch {
        logger.trace("ENTRY:");
        logger.debug("invokeExecQuery: rawkey={}, curRight={}"
                + ", qid={}, doAction={}", rawkey, curRight, qid, doAction);
        ExecQueryReturn eqr = new ExecQueryReturn();
        RQVNode<E> snode;
        rtLockR(); // to avoid race condition with removeKey()
        try {
            snode = getVNode(rawkey);
            if (snode == null) {
                logger.warn("vnode for rawkey not exists on {}", this.trans.getEndpoint());
                throw new NoSuchKeyException(rawkey + ", " + keyHash);
            }
            Node node = snode.getDdllNode();
            node.lock();
            if (node.getMode() == Mode.GRACE || node.getMode() == Mode.OUT) {
                // GRACEならばもう少しスマートな方法がありそうだが，とりあえず．
                node.unlock();
                throw new NoSuchKeyException(rawkey + " has been deleted");
            }
            eqr.right = node.getRight();
            eqr.left = node.getLeft();
            node.unlock();
            if (curRight != null && !curRight.equals(eqr.right)) {
                throw new RightNodeMismatch(eqr.right);
            }
            if (snode.getMode() == VNodeMode.DELETING) {
                // removeKey()を実行中だが，level0のDDLLノードはまだ削除されていない場合，
                // execQueryは実行しない．
                logger.debug("invokeExecQuery: ignore deleting node {}", snode);
                return eqr;
            }
            if (!doAction) {
                return eqr;
            }
        } finally {
            rtUnlockR();
        }
        
        RemoteValue<?> r = null;
        if (TransOptions.deliveryMode(opts) == DeliveryMode.ACCEPT_ONCE) {
            r = snode.store.get(qid);
            if (r != null) {
                eqr.key = r;
                logger.debug("invokeExecQuery returns {}", eqr);
                return eqr;
            }
        }
        RemoteValue<?> rval = execQuery(rawkey, query);
        if (TransOptions.deliveryMode(opts) == DeliveryMode.ACCEPT_ONCE) {
        		snode.store.put(qid, rval);
        }
        eqr.key = rval;
        logger.debug("invokeExecQuery returns {}", eqr);
        return eqr;
    }

    /**
     * Range Query実行時に呼び出されるメソッド．
     * 
     * @param key the key 
     * @param query the query object
     * @return the result of the query (remote values).
     */
    public/*protected*/RemoteValue<?> execQuery(Comparable<?> key, Object query) {
        logger.trace("ENTRY:");
        try {
            logger.debug("execQuery: key={}, query={}", key, query);
            if (execQueryCallback == null) {
                return new RemoteValue<Object>(peerId, key);
            }
            return execQueryCallback.rqExecQuery(key, query);
        } finally {
            logger.trace("EXIT:");
        }
    }

    /**
     * a TimerTask for purging stale QueryId entries
     */
    class PurgeTask implements Runnable {
        @Override
        public void run() {
            rtLockR();
            for (RingVNode<E> _snode : keyHash.values()) {
                RQVNode<E> snode = (RQVNode<E>) _snode;
                snode.store.removeExpired(QID_EXPIRE);
            }
            rtUnlockR();
            //logger.debug("msgframe dump: {}", msgframe);
        }
    }
}
